from starlette.config import Config

config = Config()


class Settings:
    THREAD_POOL_WORKER_COUNT = config('THREAD_POOL_WORKER_COUNT', cast=int, default=None)
    PREFETCH_COUNT = config('PREFETCH_COUNT', cast=int, default=1)


settings = Settings()
