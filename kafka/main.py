import asyncio

from kafka import broker


def run(target: str) -> None:
    servers = {
        "broker": broker.run_broker,
    }
    try:
        asyncio.run(servers[target]())
    except KeyError:
        raise ValueError(
            f"Unknown target: {target}. Available targets: {', '.join(servers.keys())}"
        )


if __name__ == "__main__":
    run("broker")
