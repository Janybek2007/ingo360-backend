from fastapi import WebSocket


class ConnectionManager:
    def __init__(self):
        self.active_connections: dict[int, WebSocket] = {}

    async def connect(self, user_id: int, websocket: WebSocket):
        await websocket.accept()

        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].close()
            except Exception:
                pass

        self.active_connections[user_id] = websocket

    def disconnect(self, user_id: int):
        if user_id in self.active_connections:
            del self.active_connections[user_id]

    async def send_token_invalidation(self, user_id: int):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(
                    {
                        "type": "token_invalidated",
                        "message": "Ваш сеанс был закрыт из-за входа с другого устройства",
                    }
                )
                await self.active_connections[user_id].close()
            except Exception:
                pass
            finally:
                self.disconnect(user_id)

    async def send_user_deactivation(self, user_id: int):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(
                    {
                        "type": "account_deactivated",
                        "message": "Ваш аккаунт был деактивирован",
                    }
                )
                await self.active_connections[user_id].close()
            except Exception:
                ...
            finally:
                self.disconnect(user_id)

    async def send_company_access_revoked(self, user_id: int, access_type: str):
        if user_id in self.active_connections:
            try:
                await self.active_connections[user_id].send_json(
                    {
                        "type": "company_access_revoked",
                        "access_type": access_type,
                        "message": f"Доступ к {access_type} был отозван для вашей компании",
                    }
                )
            except Exception:
                pass

    async def notify_users(self, user_ids: list[int], notification_type: str, **kwargs):
        for user_id in user_ids:
            if user_id in self.active_connections:
                try:
                    message_data = {"type": notification_type, **kwargs}
                    await self.active_connections[user_id].send_json(message_data)
                except Exception:
                    pass

    # async def send_import_notification(
    #     self, user_id: int, task_id: str, status: str, result: dict = None
    # ):
    #     if user_id in self.active_connections:
    #         await self.notify_users(
    #             user_ids=[user_id],
    #             notification_type="import_status",
    #             task_id=task_id,
    #             status=status,
    #             result=result,
    #         )


connection_manager = ConnectionManager()
