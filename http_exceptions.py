from fastapi import HTTPException, status
from fastapi.responses import JSONResponse


class ServiceException(HTTPException):
    status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
    message = "Internal Server Error"
    code = "INTERNAL_SERVER_ERROR"
    meta = {}

    def __init__(self, message=None, code=None, errors=None, status_code=None, meta=None):
        if meta:
            self.meta = meta

        if message:
            self.message = message
        if status_code:
            self.status_code = status_code

        self.payload = {"message": self.message}

        if errors:
            self.payload["errors"] = errors

        if code:
            self.code = code

        if self.code:
            self.payload["code"] = self.code

        super().__init__(status_code=self.status_code, detail=self.payload)

    def to_response(self, is_json=True):
        return (
            JSONResponse(
                status_code=self.status_code,
                content={"detail": {i: v for i, v in self.payload.items() if v is not None}},
            )
            if is_json
            else dict(detail=self.payload)
        )


class RequestError(ServiceException):
    status_code = status.HTTP_400_BAD_REQUEST
    message = "Bad request"
    code = "BAD_REQUEST"


class AuthenticationFailedError(ServiceException):
    status_code = status.HTTP_401_UNAUTHORIZED
    message = "Authentication Failed."
    code = "AUTHENTICATION_FAILED"


class PermissionDeniedError(ServiceException):
    status_code = status.HTTP_403_FORBIDDEN
    message = "You do not have permission to perform this action."
    code = "PERMISSION_DENIED"


class ValidationError(ServiceException):
    status_code = status.HTTP_422_UNPROCESSABLE_ENTITY
    message = "Validation Error"
    code = "VALIDATION_ERROR"


class MethodNotAllowed(ServiceException):
    status_code = status.HTTP_405_METHOD_NOT_ALLOWED
    message = "Method Not Allowed"
    code = "METHOD_NOT_ALLOWED"


class NotFound(ServiceException):
    status_code = status.HTTP_404_NOT_FOUND
    message = "API Not Found"
    code = "NOT_FOUND"


class ConflictError(ServiceException):
    status_code = status.HTTP_409_CONFLICT
    message = "Conflict Error"
    code = "CONFLICT_ERROR"


class ServiceUnavailableException(ServiceException):
    status_code = status.HTTP_503_SERVICE_UNAVAILABLE
    message = "Service Unavailable"
    code = "SERVICE_UNAVAILABLE"
