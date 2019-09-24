
/**
 *    Copyright (C) 2018-present MongoDB, Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
 *    it under the terms of the Server Side Public License, version 1,
 *    as published by MongoDB, Inc.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    Server Side Public License for more details.
 *
 *    You should have received a copy of the Server Side Public License
 *    along with this program. If not, see
 *    <http://www.mongodb.com/licensing/server-side-public-license>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the Server Side Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kCommand

#include "mongo/platform/basic.h"

#include "mongo/base/status.h"
#include "mongo/db/commands.h"
#include "mongo/db/operation_context.h"
#include "mongo/s/catalog_cache.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/util/scopeguard.h"
#include "mongo/s/commands/cluster_commands_helpers.h"

namespace mongo {
namespace {

class DropCmd : public BasicCommand {
public:
    DropCmd() : BasicCommand("drop") {}

    AllowedOnSecondary secondaryAllowed(ServiceContext*) const override {
        return AllowedOnSecondary::kAlways;
    }

    bool adminOnly() const override {
        return false;
    }

    bool supportsWriteConcern(const BSONObj& cmd) const override {
        return true;
    }

    void addRequiredPrivileges(const std::string& dbname,
                               const BSONObj& cmdObj,
                               std::vector<Privilege>* out) const override {
        ActionSet actions;
        actions.addAction(ActionType::dropCollection);
        out->push_back(Privilege(parseResourcePattern(dbname, cmdObj), actions));
    }

    bool run(OperationContext* opCtx,
             const std::string& dbname,
             const BSONObj& cmdObj,
             BSONObjBuilder& result) override {

        const NamespaceString nss(CommandHelpers::parseNsCollectionRequired(dbname, cmdObj));

        // direct path for host mode
        if (serverGlobalParams.hostModeRouterEnabled) {
            _dropUnshardedCollectionFromShard(
                opCtx, Grid::get(opCtx)->shardRegistry()->getHostShard()->getId(), nss, &result);
            return true;
        }

        // Invalidate the routing table cache entry for this collection so that we reload it the
        // next time it is accessed, even if sending the command to the config server fails due
        // to e.g. a NetworkError.
        ON_BLOCK_EXIT(
            [opCtx, nss] { Grid::get(opCtx)->catalogCache()->invalidateShardedCollection(nss); });

        auto configShard = Grid::get(opCtx)->shardRegistry()->getConfigShard();
        auto cmdResponse = uassertStatusOK(configShard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting(ReadPreference::PrimaryOnly),
            "admin",
            CommandHelpers::appendMajorityWriteConcern(CommandHelpers::appendPassthroughFields(
                cmdObj, BSON("_configsvrDropCollection" << nss.toString()))),
            Shard::RetryPolicy::kIdempotent));

        CommandHelpers::filterCommandReplyForPassthrough(cmdResponse.response, &result);
        
        return true;
    }

private:
    static void _dropUnshardedCollectionFromShard(OperationContext* opCtx,
                                                  const ShardId& shardId,
                                                  const NamespaceString& nss,
                                                  BSONObjBuilder* result) {

        const auto shardRegistry = Grid::get(opCtx)->shardRegistry();

        const auto dropCommandBSON = [shardRegistry, opCtx, &shardId, &nss] {
            BSONObjBuilder builder;
            builder.append("drop", nss.coll());

            if (!opCtx->getWriteConcern().usedDefault) {
                builder.append(WriteConcernOptions::kWriteConcernField,
                               opCtx->getWriteConcern().toBSON());
            }

            return builder.obj();
        }();

        const auto shard = uassertStatusOK(shardRegistry->getShard(opCtx, shardId));

        auto cmdDropResult = uassertStatusOK(shard->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            nss.db().toString(),
            dropCommandBSON,
            Shard::RetryPolicy::kIdempotent));

        // If the collection doesn't exist, consider the drop a success.
        if (cmdDropResult.commandStatus == ErrorCodes::NamespaceNotFound) {
            return;
        }

        uassertStatusOK(cmdDropResult.commandStatus);
        if (!cmdDropResult.writeConcernStatus.isOK()) {
            appendWriteConcernErrorToCmdResponse(
                shardId, cmdDropResult.response["writeConcernError"], *result);
        }
    };

} clusterDropCmd;

}  // namespace
}  // namespace mongo
