"""Custom exception hierarchy for tap-sendgrid HTTP error handling."""


class SendgridError(Exception):
    """Base tap-sendgrid exception."""


class SendgridBadRequestError(SendgridError):
    """400 error."""


class SendgridAuthenticationError(SendgridError):
    """401/403 error."""


class SendgridNotFoundError(SendgridError):
    """404 error."""


class SendgridRateLimitError(SendgridError):
    """429 error."""


class SendgridServerError(SendgridError):
    """5xx error."""


ERROR_CODE_EXCEPTION_MAPPING = {
    400: SendgridBadRequestError,
    401: SendgridAuthenticationError,
    403: SendgridAuthenticationError,
    404: SendgridNotFoundError,
    429: SendgridRateLimitError,
}
