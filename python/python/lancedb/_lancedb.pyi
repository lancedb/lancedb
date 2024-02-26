from typing import Optional

class Connection(object):
    async def table_names(self) -> list[str]: ...

async def connect(
    uri: str,
    api_key: Optional[str],
    region: Optional[str],
    host_override: Optional[str],
    read_consistency_interval: Optional[float],
) -> Connection: ...
