import asyncio
from uuid import UUID


class RequestManager:

    def __init__(self):
        self.pending_requests: dict[UUID, asyncio.Future] = {}


    def create_request( self, request_id: UUID) -> asyncio.Future:

        future = asyncio.get_running_loop().create_future()
        self.pending_requests[request_id] = future

        return future



    def resolve_request(self, request_id: UUID, result) -> bool:
        future = self.pending_requests.get(request_id)

        if future is None:
            return False

        if future.done():
            return False

        future.set_result(result)

        del self.pending_requests[request_id]

        return True



    def remove_request(self, request_id: UUID):
        self.pending_requests.pop(request_id,None)