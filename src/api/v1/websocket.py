from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from sqlalchemy import select

from src.websocket.connection_manager import connection_manager
from src.db.models import AccessToken, User
from src.db.session import db_session

router = APIRouter()


async def get_user_from_token(token: str) -> User | None:
    async for session in db_session.get_session():
        access_token_db = AccessToken.get_db(session)
        access_token = await access_token_db.get_by_token(token)

        if not access_token:

            return None

        result = await session.execute(
            select(User).where(User.id == access_token.user_id)
        )
        user = result.scalar_one_or_none()

        if not user:
            return None

        return user


@router.websocket("/ws/notifications")
async def websocket_notifications_endpoint(websocket: WebSocket, token: str):
    user = None
    try:
        user = await get_user_from_token(token)

        if not user:
            await websocket.close(code=4001, reason="Invalid token")
            return

        await connection_manager.connect(user.id, websocket)

        try:
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
        except WebSocketDisconnect:
            pass

    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if user:
            connection_manager.disconnect(user.id)
