from datetime import timedelta

default_args = {
    "depends_on_past": False,
    "email": ["gscolombo404@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30)
}