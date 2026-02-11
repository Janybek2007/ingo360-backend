from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from src.api.utils.get_user_from_token import get_user_from_token
from src.services.websocket import build_tasks_payload, soft_delete_task
from src.websocket.connection_manager import connection_manager

router = APIRouter()


@router.websocket("/ws/notifications")
async def websocket_notifications_endpoint(websocket: WebSocket, token: str):
    user = None
    try:
        user = await get_user_from_token(token)

        if not user:
            await websocket.close(code=4001, reason="Invalid token")
            return

        await connection_manager.connect(user.id, websocket)
        await websocket.send_json(await build_tasks_payload(user.id))

        try:
            while True:
                data = await websocket.receive_text()
                if data == "ping":
                    await websocket.send_json({"type": "pong"})
                    continue

                if data.startswith("task_remove:"):
                    task_id = data.split(":", 1)[1]
                    removed = await soft_delete_task(user.id, task_id)
                    await websocket.send_json(
                        {
                            "type": "task_removed",
                            "task_id": task_id,
                            "ok": removed,
                        }
                    )
                    continue
        except WebSocketDisconnect:
            pass

    except Exception as e:
        print(f"WebSocket error: {e}")
    finally:
        if user:
            connection_manager.disconnect(user.id)
