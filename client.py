import argparse
import socket


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("host", type=str)
    parser.add_argument("port", type=int)
    gp = parser.add_mutually_exclusive_group(required=True)
    gp.add_argument("--get", metavar="KEY")
    gp.add_argument("--insert", nargs=2, metavar=("KEY", "VALUE"))
    gp.add_argument("--update", nargs=2, metavar=("KEY", "VALUE"))
    gp.add_argument("--delete", metavar="KEY")

    return parser.parse_args()


def get_response(host: str, port: str, data: bytes):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(30)
        s.connect((host, port))
        s.send(data)
        while True:
            data = s.recv(1024)
            print(data)
            s.close()
            break
    except Exception as e:
        print(str(e))


def main():
    args = parse_args()
    host, port = args.host, args.port
    if args.get:
        data = f"GET\n{args.get}\n"
    elif args.insert:
        k, v = tuple(args.insert)
        data = f"INSERT\n{k}\n{v}\n"
    elif args.update:
        k, v = tuple(args.insert)
        data = f"UPDATE\n{k}\n{v}\n"
    elif args.delete:
        data = f"DELETE\n{args.delete}\n"
    get_response(host, port, data.encode("utf-8"))


if __name__ == "__main__":
    main()
