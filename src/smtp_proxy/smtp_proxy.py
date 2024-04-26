#!/user/bin/env python3

import click
import logging
import asyncio
import email.message
import email.parser
import signal
import os

from aiosmtpd.controller import Controller
from aiosmtpd.smtp import Envelope
from async_sendgrid import SendgridAPI
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

# from azure.identity import DeviceCodeCredential
# from msgraph import GraphServiceClient
# from msgraph.generated.users.item.send_mail.send_mail_post_request_body import (
#     SendMailPostRequestBody,
# )
# from msgraph.generated.models.message import Message
# from msgraph.generated.models.item_body import ItemBody
# from msgraph.generated.models.body_type import BodyType
# from msgraph.generated.models.recipient import Recipient
# from msgraph.generated.models.email_address import EmailAddress


logging.basicConfig(format="[%(levelname)-8s] %(message)s", level=logging.WARNING)
log = logging.getLogger(__name__)


MAX_RETRIES = 5


class BaseHandler:
    queue = asyncio.Queue()
    continueLoop = True

    def __init__(self, **kwargs):
        self.client = self.getClient(**kwargs)

    def getClient(**kwargs):
        raise NotImplementedError()

    def processPayload(self, mailArgs):
        raise NotImplementedError()

    async def sendMail(self, payload):
        raise NotImplementedError()

    async def handle_DATA(self, server, session, envelope: Envelope):
        log.debug("DATA receieved, adding to queue")
        self.queue.put_nowait(envelope)
        return "250 OK"

    async def getMailFromQueue(self):
        log.debug("Waiting on Queue...")
        envelope = await self.queue.get()
        log.debug("Received envelope")
        msg = email.parser.BytesParser(email.message.EmailMessage).parsebytes(envelope.content)

        return {
            "mail_from": envelope.mail_from,
            "rcpt_tos": envelope.rcpt_tos,
            "subject": msg["Subject"],
            "body": msg.get_payload(),
        }

    async def handleQueue(self):
        while True:
            mailArgs = await self.getMailFromQueue()

            payload = self.processPayload(mailArgs)

            retries = 0
            className = type(self).__name__
            while retries < MAX_RETRIES:
                try:
                    log.debug(f"Sending payload via {className}")
                    response = await self.sendMail(payload)

                    # TODO: Adapt to MS Graph response value + add wait until support
                    if response.status_code < 400:
                        log.debug(f"Payload Sent!")
                        break
                    else:
                        raise Exception(f"[{response.status_code}: {response.text}]")
                except Exception as err:
                    retries += 1
                    errorMsg = f"Could not process your {className} message {mailArgs['subject']} to {mailArgs['rcpt_tos']}: {err}"
                    log.debug(err)
                    if retries == MAX_RETRIES:
                        log.error(f"Retry limit reached, giving up on {errorMsg}!")
                    else:
                        log.warning(f"Retry attempt {retries}: {errorMsg}")
                        await asyncio.sleep(1 * retries)
            self.queue.task_done()


class SendgridHandler(BaseHandler):
    def getClient(self, **kwargs):
        environKey = os.environ.get("SENDGRID_API_KEY")
        return SendgridAPI(kwargs.get("sendgrid_api_key", environKey))

    def processPayload(self, mailArgs):
        sg_msg = Mail(
            from_email=mailArgs["mail_from"],
            to_emails=mailArgs["rcpt_tos"],
            subject=mailArgs["subject"],
            html_content=mailArgs["body"],
        )
        return sg_msg

    async def sendMail(self, payload):
        return await self.client.send(payload)


class MsGraphHandler(BaseHandler):
    pass
    # def getClient(self, **kwargs):
    #     client_id = kwargs["msgraph_client_id"]
    #     tenant_id = kwargs["msgraph_tenant_id"]
    #     graph_scopes = ["Mail.Send"]

    #     device_code_credential = DeviceCodeCredential(client_id, tenant_id=tenant_id)
    #     return GraphServiceClient(device_code_credential, graph_scopes)

    # def processPayload(self, mailArgs):
    #     message = Message()
    #     message.subject = mailArgs["subject"]

    #     message.body = ItemBody()
    #     message.body.content_type = BodyType.Text
    #     message.body.content = mailArgs["body"]

    #     message.to_recipients = []
    #     for rcpt_to in mailArgs["rcpt_tos"]:
    #         to_recipient = Recipient()
    #         to_recipient.email_address = EmailAddress()
    #         to_recipient.email_address.address = rcpt_to
    #         message.to_recipients.append(to_recipient)

    #     request_body = SendMailPostRequestBody()
    #     request_body.message = message

    #     return request_body

    # def sendMail(self, payload):
    #     # TODO: check what the return value actually is here
    #     return self.client.me.send_mail.post(body=payload)


class SmtpProxyServer:
    handler: BaseHandler
    controller: Controller
    asyncTasks = []
    maxConcurrentRequests = 1

    def __init__(self, **kwargs):
        if kwargs.get("msgraph"):
            self.handler = MsGraphHandler(**kwargs)
        else:
            self.handler = SendgridHandler(**kwargs)

        self.controller = Controller(self.handler, hostname=kwargs["hostname"], port=kwargs["port"])
        self.maxConcurrentRequests = kwargs.get("max_concurrent_requests", 1)

    async def tick(self):
        await self.handler.queue.join()
        await asyncio.sleep(0.1)

    def start(self):
        asyncio.run(self.runServer())

    def _initTasks(self):
        # Create forever running tasks that consume the queue
        # based off max concurrent requests allowed
        self.asyncTasks = []
        for _ in range(self.maxConcurrentRequests):
            self.asyncTasks.append(asyncio.create_task(self.handler.handleQueue()))

    async def runServer(self):
        # Run the event loop in a separate thread.
        self.controller.start()

        self._initTasks()

        # Graceful stop on termination,
        # wait for the queue to clear before terminating
        def stop(signum, frame):
            self.handler.continueLoop = False
            self.controller.stop()

        signal.signal(signal.SIGINT, stop)
        signal.signal(signal.SIGTERM, stop)

        while self.handler.continueLoop:
            await self.tick()

        log.debug("Graceful Exit!")


@click.command()
@click.option("--hostname", required=True)
@click.option("--port", required=True)
@click.option("--max-concurrent-requests", default=4, type=int)
@click.option("-d", "--debug", is_flag=True)
@click.option("--sendgrid", is_flag=True)
@click.option("--sendgrid-api-key")
@click.option("--msgraph", is_flag=True)
@click.option("--msgraph-client-id")
@click.option("--msgraph-tenant-id")
def main(**kwargs):
    if kwargs.get("debug"):
        log.setLevel(logging.DEBUG)

    server = SmtpProxyServer(**kwargs)
    server.start()


if __name__ == "__main__":
    main()
