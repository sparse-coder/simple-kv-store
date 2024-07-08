import argparse
import asyncio

from kv_store import KVStore

"""
format -> opcode \n KEY \n VALUE
"""

lock = asyncio.Lock()
store: KVStore = None
encoding = "utf-8"


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", type=str, default="kvstore")
    parser.add_argument("--host", type=str, default="localhost")
    parser.add_argument("--port", type=int, default="10000")

    return parser.parse_args()


def process(request: bytes) -> str:
    op, _, kv = request.partition(b"\n")

    op = op.decode(encoding).upper()
    kv = [item.decode(encoding) for item in kv.splitlines(keepends=False)]

    if op == "GET":
        x = store.get(kv[0])
        return x
    elif op == "INSERT" or op == "UPDATE":
        store.insert_or_update(kv[0], kv[1])
        return
    elif op == "DELETE":
        store.delete(kv[0])
        return
    else:
        return "invalid operation"


async def handle_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    request = await reader.read(8 * 1024)
    try:
        print(f"request: {request}")
        async with lock:
            resp = process(request)
            resp = (f"ok\n{resp}\n" if resp else "ok\n").encode(encoding)
        print(f"response: {resp}")
        writer.write(resp)
    except Exception as e:
        print(str(e))
        resp = b"error\n"
        writer.write(resp)


async def main(file, host, port):
    server = await asyncio.start_server(handle_request, host=host, port=port)
    print("serving from `localhost:10000`")
    global store
    store = KVStore.open(file)
    store.load()
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    try:
        args = parse_args()
        asyncio.run(main(**vars(args)))
    except KeyboardInterrupt:
        print("exiting!")
    except Exception as e:
        print("An error occured!", str(e))
