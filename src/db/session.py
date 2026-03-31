from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.core.settings import settings


class DBSession:
    def __init__(self, db_url: str):
        self.engine = create_async_engine(
            url=db_url,
            pool_size=20,
            max_overflow=10,
            pool_pre_ping=True,
            pool_recycle=3600,
        )
        self.session_factory = async_sessionmaker(
            bind=self.engine, expire_on_commit=False, autocommit=False
        )

    async def get_session(self) -> AsyncSession:
        async with self.session_factory() as session:
            yield session

    async def dispose(self):
        await self.engine.dispose()


db_session = DBSession(db_url=settings.DATABASE_URL)
