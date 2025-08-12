import logging


def error_message_details(error: Exception) -> str:
    import sys

    _, _, exc_tb = sys.exc_info()
    if exc_tb:
        file_name = exc_tb.tb_frame.f_code.co_filename
        line_no = exc_tb.tb_lineno
    else:
        file_name = "unknown_file"
        line_no = "unknown line"

    error_message = f"Error occurred in Python script {file_name} at line number {line_no}: {str(error)}"

    logging.error(error_message)
    return error_message


class MyException(Exception):  # ✅ Inherit from Exception
    def __init__(self, error_message: Exception):
        detailed_error = error_message_details(error_message)
        super().__init__(detailed_error)
        self.error_message = detailed_error
