from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.settings import settings


class DBSession:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(url=db_url)
        self.session_factory = async_sessionmaker(bind=self.engine, expire_on_commit=False)

    async def get_session(self) -> AsyncSession:
        async with self.session_factory() as session:
            yield session

    async def dispose(self):
        await self.engine.dispose()


db_session = DBSession(db_url=settings.DATABASE_URL)
