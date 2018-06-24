import asyncio
import json
import time
import traceback

import websockets

from callisto2 import Callisto

c = Callisto('./data')
c.committer_thread.start()

def handle_put(db, message):
    return c.put(db, str(message))


def handle_get(db, start=None, end=None):
    if start is None:
        start = 0
    if end is None:
        end = -1
    return list(c.get(db, start, end))


def handle_delete(db):
    c.delete(db)


def handle_count(db):
    return c.count(db)


async def handle_command(command):
    verb = command['v']
    if verb == 'put':
        return handle_put(command['db'], command['message'])
    if verb == 'get':
        return handle_get(command['db'], start=command.get('start'), end=command.get('end'))
    if verb == 'count':
        return handle_count(command['db'])
    if verb == 'delete':
        return handle_delete(command['db'])
    raise ValueError(f'unknown verb {verb}')


async def handle_ws(websocket, path):
    while True:
        try:
            msg = await websocket.recv()
            command = json.loads(msg, encoding='utf-8')
        except Exception as exc:
            traceback.print_exc()
            await send_exc(websocket, exc)
            continue
        if not isinstance(command, list):
            command = [command]
        for command in command:
            try:
                t0 = time.perf_counter()
                response = await handle_command(command)
                t1 = time.perf_counter()
                await websocket.send(json.dumps({'response': response, 'command': command, 'time': t1 - t0}))
            except Exception as exc:
                traceback.print_exc()
                await send_exc(websocket, exc, command)


async def send_exc(websocket, exc, command=None):
    await websocket.send(json.dumps({
        'error': {
            'type': exc.__class__.__name__,
            'message': str(exc),
        },
        'command': command,
    }))


serve = websockets.serve(handle_ws, 'localhost', 8765)

asyncio.get_event_loop().run_until_complete(serve)
print('Serving.')
asyncio.get_event_loop().run_forever()
