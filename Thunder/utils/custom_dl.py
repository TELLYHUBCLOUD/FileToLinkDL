# Thunder/utils/custom_dl.py

import asyncio
from collections import OrderedDict
from typing import Any, AsyncGenerator, Dict

from pyrogram import Client
from pyrogram.errors import FloodWait
from pyrogram.types import Message

from Thunder.server.exceptions import FileNotFound
from Thunder.utils.file_properties import get_media
from Thunder.utils.logger import logger
from Thunder.vars import Var


class ByteStreamer:
    __slots__ = ('client', 'chat_id', '_info_cache')

    # Shared across instances per client — cache file info to avoid
    # re-fetching the same message metadata from Telegram repeatedly.
    _CACHE_MAX = 512
    _shared_cache: OrderedDict = OrderedDict()

    def __init__(self, client: Client) -> None:
        self.client = client
        self.chat_id = int(Var.BIN_CHANNEL)

    async def get_message(self, message_id: int) -> Message:
        while True:
            try:
                message = await self.client.get_messages(self.chat_id, message_id)
                break
            except FloodWait as e:
                logger.debug(f"FloodWait: get_message, sleep {e.value}s")
                await asyncio.sleep(e.value)
            except Exception as e:
                logger.debug(f"Error fetching message {message_id}: {e}", exc_info=True)
                raise FileNotFound(f"Message {message_id} not found") from e

        if not message or not message.media:
            raise FileNotFound(f"Message {message_id} not found")
        return message

    async def stream_file(
        self, message_id: int, offset: int = 0, limit: int = 0
    ) -> AsyncGenerator[bytes, None]:
        message = await self.get_message(message_id)

        chunk_offset = offset // (1024 * 1024)

        chunk_limit = 0
        if limit > 0:
            chunk_limit = ((limit + (1024 * 1024) - 1) // (1024 * 1024)) + 1

        # Prefetch buffer: read-ahead one chunk so the next chunk is
        # ready to send while the current one is being transmitted.
        PREFETCH_SIZE = 4
        buffer: asyncio.Queue = asyncio.Queue(maxsize=PREFETCH_SIZE)

        async def _producer():
            try:
                async for chunk in self.client.stream_media(
                    message, offset=chunk_offset, limit=chunk_limit
                ):
                    await buffer.put(chunk)
                await buffer.put(None)  # sentinel
            except Exception as e:
                await buffer.put(e)

        producer_task = asyncio.create_task(_producer())
        try:
            while True:
                item = await buffer.get()
                if item is None:
                    break
                if isinstance(item, Exception):
                    raise item
                yield item
        finally:
            producer_task.cancel()
            try:
                await producer_task
            except (asyncio.CancelledError, Exception):
                pass

    def get_file_info_sync(self, message: Message) -> Dict[str, Any]:
        media = get_media(message)
        if not media:
            return {"message_id": message.id, "error": "No media"}

        media_type = type(media).__name__.lower()
        file_name = getattr(media, 'file_name', None)
        mime_type = getattr(media, 'mime_type', None)

        if not file_name:
            ext_map = {
                "photo": "jpg",
                "audio": "mp3",
                "voice": "ogg",
                "video": "mp4",
                "animation": "mp4",
                "videonote": "mp4",
                "sticker": "webp",
            }
            ext = ext_map.get(media_type, "bin")
            file_name = f"Thunder_{message.id}.{ext}"

        if not mime_type:
            mime_map = {
                "photo": "image/jpeg",
                "voice": "audio/ogg",
                "videonote": "video/mp4",
            }
            mime_type = mime_map.get(media_type)

        return {
            "message_id": message.id,
            "file_size": getattr(media, 'file_size', 0) or 0,
            "file_name": file_name,
            "mime_type": mime_type,
            "unique_id": getattr(media, 'file_unique_id', None),
            "media_type": media_type
        }

    async def get_file_info(self, message_id: int) -> Dict[str, Any]:
        # Check shared cache first
        cache_key = f"{self.chat_id}:{message_id}"
        if cache_key in ByteStreamer._shared_cache:
            ByteStreamer._shared_cache.move_to_end(cache_key)
            return ByteStreamer._shared_cache[cache_key]
        try:
            message = await self.get_message(message_id)
            info = self.get_file_info_sync(message)
            # Store in cache
            ByteStreamer._shared_cache[cache_key] = info
            if len(ByteStreamer._shared_cache) > ByteStreamer._CACHE_MAX:
                ByteStreamer._shared_cache.popitem(last=False)
            return info
        except Exception as e:
            logger.debug(f"Error getting file info for {message_id}: {e}", exc_info=True)
            return {"message_id": message_id, "error": str(e)}
