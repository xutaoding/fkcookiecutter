import os
import sys
import string
import random

from fkcookiecutter.apps.user.tasks.task_send_demo import test_flask_send_message


if __name__ == '__main__':
    for _ in range(1000000):
        uid = ''.join(random.choices(string.digits + string.ascii_letters, k=32))
        test_flask_send_message.delay(task_uid=uid)

