if __name__ == '__main__':
    import os
    import sys

    from api import app

    os.chdir(os.path.dirname(__file__))

    params = dict(
        app=app,
        env_file='local.env',
        port=2127,
        log_config='server/logging-config.json',
        )

    try:
        # If on Windows, set AsyncIO loop policy compatible with Psycopg
        if sys.platform == 'win32':
            import asyncio

            from uvicorn import Config, Server

            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

            config = Config(**params, loop='asyncio')
            server = Server(config)
            loop.run_until_complete(server.serve())
        else:
            from uvicorn import run

            run(**params)
    except KeyboardInterrupt:
        pass
