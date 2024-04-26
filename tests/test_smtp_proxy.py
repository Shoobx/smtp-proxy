import pytest
import os
from smtp_proxy.smtp_proxy import SmtpProxyServer, SendgridHandler

from unittest import mock
from async_sendgrid import SendgridAPI
from aiosmtpd.smtp import Envelope


def test_smtp_proxy_sendgrid_start():
    server = SmtpProxyServer(
        hostname="localtest", port=42, max_concurrent_requests=5, sendgrid_api_key="test"
    )

    assert server.handler.client.api_key == "test"

    # Don't keep the server alive, just initialize it
    server.controller.start = mock.Mock()
    server.handler.continueLoop = False

    server.start()

    # Server is initialized with the correct handler, client, and number of tasks
    assert len(server.asyncTasks) == 5
    assert isinstance(server.controller.handler, SendgridHandler)
    assert isinstance(server.handler.client, SendgridAPI)

def test_smtp_proxy_sendgrid_start_env_var():
    os.environ['SENDGRID_API_KEY'] = "envTest"
    server = SmtpProxyServer(
        hostname="localtest", port=42, max_concurrent_requests=5
    )

    assert server.handler.client.api_key == "envTest"

    # Don't keep the server alive, just initialize it
    server.controller.start = mock.Mock()
    server.handler.continueLoop = False

    server.start()

    # Server is initialized with the correct handler, client, and number of tasks
    assert len(server.asyncTasks) == 5
    assert isinstance(server.controller.handler, SendgridHandler)
    assert isinstance(server.handler.client, SendgridAPI)


@pytest.mark.asyncio
async def test_smtp_proxy_sendgrid_queue():
    server = SmtpProxyServer(
        hostname="localtest", port=42, max_concurrent_requests=5, sendgrid_api_key="test"
    )

    # mock the api request
    sendMock = mock.AsyncMock(return_value=mock.Mock(status_code=202))
    server.handler.client.send = sendMock

    # Mock task init + adding Envelopes to the queue
    # received by the smtp handler
    server._initTasks()

    for i in range(5):
        envelope = Envelope()
        envelope.mail_from = "sender@example.com"
        envelope.content = bytes(
            (
                "From: sender@example.com\r\n"
                "To: receiver@example.com\r\n"
                "Subject: This is a test subject\r\n"
                "This is a test message."
            ),
            "utf-8",
        )
        envelope.rcpt_tos = ["receiver@example.com"]
        await server.handler.handle_DATA(None, None, envelope)

    # Nothing happens yet, until we handle the queue
    assert not sendMock.called
    await server.tick()

    # Queue was handled!
    assert sendMock.called
    assert len(sendMock.call_args_list) == 5
    for mockArgs in sendMock.call_args_list:
        mail = mockArgs[0][0]
        assert mail.from_email.email == "sender@example.com"
        assert mail.personalizations[0].tos[0]["email"] == "receiver@example.com"
        assert mail.subject.subject == "This is a test subject"
        assert mail.contents[0].content == "This is a test message."

    # Additional calls are *not* made the next time
    # the server processes the queue
    await server.tick()
    assert len(sendMock.call_args_list) == 5

    for task in server.asyncTasks:
        task.cancel()
