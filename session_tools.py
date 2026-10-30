import asyncio
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeout

from pyrogram import Client
from pyrogram import errors

from config import API_ID, API_HASH


def _dead_errors():
    found = []
    for name in ("AuthKeyUnregistered", "AuthKeyInvalid", "AuthKeyDuplicated",
                 "UserDeactivated", "SessionRevoked", "SessionExpired",
                 "UserDeactivatedBan"):
        exc = getattr(errors, name, None)
        if isinstance(exc, type):
            found.append(exc)
    return tuple(found) or (errors.AuthKeyUnregistered,)


DEAD_ERRORS = _dead_errors()


async def _check_async(session_string, attempts, delay):
    last = "Unknown"
    for i in range(max(1, attempts)):
        client = None
        try:
            client = Client(
                name=f"chk_{uuid.uuid4().hex[:10]}",
                api_id=API_ID,
                api_hash=API_HASH,
                session_string=session_string,
                in_memory=True,
                no_updates=True,
            )
            await client.connect()
            me = await client.get_me()
            if not me:
                raise errors.AuthKeyUnregistered("no user")
            return True, None
        except DEAD_ERRORS as exc:
            last = type(exc).__name__
        except Exception as exc:
            last = type(exc).__name__
        finally:
            if client is not None:
                try:
                    await client.disconnect()
                except Exception:
                    pass
        if i < attempts - 1:
            await asyncio.sleep(delay)
    return False, last


async def check_session_async(session_string, attempts=3, delay=0.7):
    if not session_string:
        return False, "missing"
    try:
        return await asyncio.wait_for(_check_async(session_string, attempts, delay), timeout=30)
    except asyncio.TimeoutError:
        return False, "Timeout"


def check_session(session_string, attempts=3, delay=0.7):
    if not session_string:
        return False, "missing"
    loop = asyncio.new_event_loop()
    try:
        asyncio.set_event_loop(loop)
        return loop.run_until_complete(
            asyncio.wait_for(_check_async(session_string, attempts, delay), timeout=30))
    except asyncio.TimeoutError:
        return False, "Timeout"
    except Exception as exc:
        return False, type(exc).__name__
    finally:
        try:
            loop.close()
        except Exception:
            pass


def check_many(items, key="phone", workers=8, budget=45, attempts=2, delay=0.5):
    alive = set()
    if not items:
        return alive

    pool = ThreadPoolExecutor(max_workers=workers)
    try:
        futures = {
            pool.submit(check_session, item.get("session_string"), attempts, delay): item
            for item in items if item.get("session_string")
        }
        deadline = time.time() + budget
        for future in futures:
            pass
        try:
            for future in __import__("concurrent.futures").futures.as_completed(futures, timeout=budget):
                item = futures[future]
                try:
                    ok, _err = future.result()
                except Exception:
                    ok = False
                if ok:
                    alive.add(item.get(key))
                if time.time() > deadline:
                    break
        except FutureTimeout:
            pass
    finally:
        pool.shutdown(wait=False)
    return alive