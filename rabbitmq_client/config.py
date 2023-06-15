from starlette.config import Config

config = Config('.env')


class Settings:
    THREAD_POOL_WORKER_COUNT = config('THREAD_POOL_WORKER_COUNT', cast=int, default=None)


settings = Settings()
