from pydantic import BaseModel, SecretStr


class BrokerConfig(BaseModel):
    username: str
    password: SecretStr
    host: str
    port: int
    vhost: str
    ssl_enabled: bool
    heartbeat: int = 60
