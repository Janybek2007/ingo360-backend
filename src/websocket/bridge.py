# import json

# from redis import asyncio as aioredis

# from src.core.settings import settings

# from .connection_manager import connection_manager


# async def redis_to_ws_bridge():
#     redis = aioredis.from_url(settings.CELERY_BROKER_URL, decode_responses=True)
#     pubsub = redis.pubsub()
#     await pubsub.subscribe("celery_tasks_notifications")

#     try:
#         async for message in pubsub.listen():
#             if message["type"] != "message":
#                 continue
#             data = json.loads(message["data"])

#             user_id = data.get("user_id")
#             task_id = data.get("task_id")
#             status = data.get("status")
#             result = data.get("result")

#             if user_id:
#                 await connection_manager.send_import_notification(
#                     user_id=int(user_id), task_id=task_id, status=status, result=result
#                 )
#     except Exception as e:
#         print(f"Ошибка в мосту WebSocket: {e}")
#     finally:
#         await pubsub.unsubscribe("celery_tasks_notifications")
#         await redis.close()
