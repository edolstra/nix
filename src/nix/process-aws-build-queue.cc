#include "command.hh"
#include "common-args.hh"
#include "store-api.hh"
#include "s3.hh"
#include "s3-binary-cache-store.hh"
#include "derivations.hh"
#include "progress-bar.hh"

#include <aws/sqs/SQSClient.h>
#include <aws/sqs/model/DeleteMessageRequest.h>
#include <aws/sqs/model/ReceiveMessageRequest.h>
#include <aws/sqs/model/SendMessageRequest.h>

#include <nlohmann/json.hpp>

using namespace nix;

struct CmdProcessAwsBuildQueue : StoreCommand
{
    std::string awsStoreUri;

    CmdProcessAwsBuildQueue()
    {
        expectArg("store-uri", &awsStoreUri);
    }

    std::string name() override
    {
        return "process-aws-build-queue";
    }

    std::string description() override
    {
        return "build derivations posted to an SQS queue";
    }

    void run(ref<Store> store) override
    {
        stopProgressBar();
        verbosity = lvlInfo;

        auto awsStore = openStore(awsStoreUri);

        auto awsStore2 = awsStore.dynamic_pointer_cast<AwsStore>();
        if (!awsStore2) throw Error("'%s' is not an aws:// store", awsStoreUri);

        auto sqsClient = awsStore2->getSQSClient();
        auto buildQueueUrl = awsStore2->getBuildQueueUrl();

        while (true) {
            // FIXME: catch AWS / nlohmann exceptions

            checkInterrupt();

            auto res = checkAws(fmt("AWS error receiving message from queue '%s'", buildQueueUrl),
                sqsClient->ReceiveMessage(
                    Aws::SQS::Model::ReceiveMessageRequest()
                    .WithQueueUrl(buildQueueUrl)
                    .WithWaitTimeSeconds(20)));

            printError("got %d messages", res.GetMessages().size());

            assert(res.GetMessages().size() <= 1);

            if (res.GetMessages().empty()) continue;

            auto & msg = res.GetMessages()[0];

            printError("got message: %s", msg.GetBody());

            // FIXME: start message visibility thread

            // FIXME: should not be necessary.
            auto s = msg.GetBody();
            s = replaceStrings(s, "&lt;", "<");
            s = replaceStrings(s, "&gt;", ">");

            auto data = nlohmann::json::parse(s);

            Path drvPath = data["drvPath"];
            std::string resultQueueUrl = data["resultQueue"];

            nlohmann::json & drvData = data["drv"];

            /* Decode the derivation from the JSON. */
            BasicDerivation drv;
            nlohmann::json & outputData = drvData["outputs"];
            for (auto i = outputData.begin(); i != outputData.end(); ++i)
                drv.outputs[i.key()] = DerivationOutput(
                    i.value()["path"],
                    i.value().value("hashAlgo", ""),
                    i.value().value("hash", ""));
            for (auto & i : drvData["inputSrcs"]) {
                std::string p = i;
                drv.inputSrcs.insert(p);
            }
            drv.platform = drvData["platform"];
            drv.builder = drvData["builder"];
            for (auto & i : drvData["args"])
                drv.args.push_back(i);
            nlohmann::json & envData = drvData["env"];
            for (auto i = envData.begin(); i != envData.end(); ++i)
                drv.env.emplace(i.key(), i.value());

            // FIXME: validate resultQueue

            /* FIXME: should use the Nix FUSE filesystem to fetch
               inputs from S3 lazily. */

            /* Copy the input closure from S3 to the local store. */
            // FIXME: signatures?
            printInfo("copying inputs...");
            copyPaths(awsStore, store, drv.inputSrcs, NoRepair, NoCheckSigs);

            /* Build the derivation locally. */
            printInfo("building...");
            auto buildResult = store->buildDerivation(drvPath, drv);

            /* Copy the outputs from the local store to S3. */
            printInfo("copying outputs...");
            copyPaths(store, awsStore, drv.outputPaths(), NoRepair, NoCheckSigs);

            // FIXME: stop message visibility thread

            printInfo("deleting message...");
            checkAws("AWS error deleting message from queue",
                sqsClient->DeleteMessage(
                    Aws::SQS::Model::DeleteMessageRequest()
                    .WithQueueUrl(buildQueueUrl)
                    .WithReceiptHandle(msg.GetReceiptHandle())));

            /* Post completion to the result queue. */
            nlohmann::json resultData;
            resultData["status"] = buildResult.status;
            resultData["errorMsg"] = buildResult.errorMsg;
            resultData["startTime"] = buildResult.startTime;
            resultData["stopTime"] = buildResult.stopTime;

            checkAws(fmt("AWS error sending message to queue '%s'", resultQueueUrl),
                sqsClient->SendMessage(
                    Aws::SQS::Model::SendMessageRequest()
                    .WithQueueUrl(resultQueueUrl)
                    .WithMessageBody(resultData.dump())));
        }
    }
};

static RegisterCommand r1(make_ref<CmdProcessAwsBuildQueue>());
