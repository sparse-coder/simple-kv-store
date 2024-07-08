import argparse

from kv_store import KVStore


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("file")
    gp = parser.add_mutually_exclusive_group()
    gp.add_argument("--get", metavar="KEY")
    gp.add_argument("--insert", nargs=2, metavar=("KEY", "VALUE"))
    gp.add_argument("--update", nargs=2, metavar=("KEY", "VALUE"))
    gp.add_argument("--delete", metavar="KEY")

    return parser.parse_args()


def main():
    args = parse_args()
    file = args.file

    store = KVStore.open(file)
    store.load()
    try:
        if args.get:
            x = store.get(args.get)
            print(x)
        elif args.insert:
            store.insert_or_update(*args.insert)
        elif args.update:
            store.insert_or_update(*args.insert)
        elif args.delete:
            store.delete(args.delete)

    except KeyError as ke:
        print(f"Key:{str(ke)} not found.")
    except Exception as e:
        print(f"An error occured: {str(e)}")


if __name__ == "__main__":
    main()
