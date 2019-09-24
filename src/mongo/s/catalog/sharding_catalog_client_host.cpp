
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

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/sharding_catalog_client_host.h"

#include "mongo/base/status.h"
#include "mongo/db/repl/optime.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_config_version.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/catalog/type_tags.h"
#include "mongo/stdx/memory.h"
#include "mongo/s/grid.h"
#include "mongo/db/commands.h"

namespace mongo {

using std::string;
using std::vector;

const ReadPreferenceSetting kConfigPrimaryPreferredSelector(ReadPreference::PrimaryPreferred,
                                                            TagSet{});

ShardingCatalogClientHost::ShardingCatalogClientHost(
    std::unique_ptr<DistLockManager> distLockManager)
    : _distLockManager(std::move(distLockManager)) {}

ShardingCatalogClientHost::~ShardingCatalogClientHost() = default;

void ShardingCatalogClientHost::startup() {
    if (_distLockManager) {
        _distLockManager->startUp();
    }
}

void ShardingCatalogClientHost::shutDown(OperationContext* opCtx) {
    if (_distLockManager) {
        _distLockManager->shutDown(opCtx);
    }
}

StatusWith<repl::OpTimeWith<DatabaseType>> ShardingCatalogClientHost::getDatabase(
    OperationContext* opCtx, const string& dbName, repl::ReadConcernLevel readConcernLevel) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<repl::OpTimeWith<std::vector<DatabaseType>>> ShardingCatalogClientHost::getAllDBs(
    OperationContext* opCtx, repl::ReadConcernLevel readConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<repl::OpTimeWith<CollectionType>> ShardingCatalogClientHost::getCollection(
    OperationContext* opCtx, const NamespaceString& nss, repl::ReadConcernLevel readConcernLevel) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<std::vector<CollectionType>> ShardingCatalogClientHost::getCollections(
    OperationContext* opCtx,
    const string* dbName,
    repl::OpTime* optime,
    repl::ReadConcernLevel readConcernLevel) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

std::vector<NamespaceString> ShardingCatalogClientHost::getAllShardedCollectionsForDb(
    OperationContext* opCtx, StringData dbName, repl::ReadConcernLevel readConcern) {
    return {};
}

StatusWith<std::vector<std::string>> ShardingCatalogClientHost::getDatabasesForShard(
    OperationContext* opCtx, const ShardId& shardName) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<std::vector<ChunkType>> ShardingCatalogClientHost::getChunks(
    OperationContext* opCtx,
    const BSONObj& filter,
    const BSONObj& sort,
    boost::optional<int> limit,
    repl::OpTime* opTime,
    repl::ReadConcernLevel readConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<std::vector<TagsType>> ShardingCatalogClientHost::getTagsForCollection(
    OperationContext* opCtx, const NamespaceString& nss) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<repl::OpTimeWith<std::vector<ShardType>>> ShardingCatalogClientHost::getAllShards(
    OperationContext* opCtx, repl::ReadConcernLevel readConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

bool ShardingCatalogClientHost::runUserManagementWriteCommand(OperationContext* opCtx,
                                                              const std::string& commandName,
                                                              const std::string& dbname,
                                                              const BSONObj& cmdObj,
                                                              BSONObjBuilder* result) {
    BSONObj cmdToRun = cmdObj;
    {
        // Make sure that if the command has a write concern that it is w:1 or w:majority, and
        // convert w:1 or no write concern to w:majority before sending.
        WriteConcernOptions writeConcern;
        writeConcern.reset();

        BSONElement writeConcernElement = cmdObj[WriteConcernOptions::kWriteConcernField];
        bool initialCmdHadWriteConcern = !writeConcernElement.eoo();
        if (initialCmdHadWriteConcern) {
            Status status = writeConcern.parse(writeConcernElement.Obj());
            if (!status.isOK()) {
                return CommandHelpers::appendCommandStatusNoThrow(*result, status);
            }

            if (!(writeConcern.wNumNodes == 1 ||
                  writeConcern.wMode == WriteConcernOptions::kMajority)) {
                return CommandHelpers::appendCommandStatusNoThrow(
                    *result,
                    {ErrorCodes::InvalidOptions,
                     str::stream() << "Invalid replication write concern. User management write "
                                      "commands may only use w:1 or w:'majority', got: "
                                   << writeConcern.toBSON()});
            }
        }

        writeConcern.wMode = WriteConcernOptions::kMajority;
        writeConcern.wNumNodes = 0;

        BSONObjBuilder modifiedCmd;
        if (!initialCmdHadWriteConcern) {
            modifiedCmd.appendElements(cmdObj);
        } else {
            BSONObjIterator cmdObjIter(cmdObj);
            while (cmdObjIter.more()) {
                BSONElement e = cmdObjIter.next();
                if (WriteConcernOptions::kWriteConcernField == e.fieldName()) {
                    continue;
                }
                modifiedCmd.append(e);
            }
        }
        modifiedCmd.append(WriteConcernOptions::kWriteConcernField, writeConcern.toBSON());
        cmdToRun = modifiedCmd.obj();
    }

    auto response =
        Grid::get(opCtx)->shardRegistry()->getHostShard()->runCommandWithFixedRetryAttempts(
            opCtx,
            ReadPreferenceSetting{ReadPreference::PrimaryOnly},
            dbname,
            cmdToRun,
            Shard::kDefaultConfigCommandTimeout,
            Shard::RetryPolicy::kNotIdempotent);

    if (!response.isOK()) {
        return CommandHelpers::appendCommandStatusNoThrow(*result, response.getStatus());
    }
    if (!response.getValue().commandStatus.isOK()) {
        return CommandHelpers::appendCommandStatusNoThrow(*result,
                                                          response.getValue().commandStatus);
    }
    if (!response.getValue().writeConcernStatus.isOK()) {
        return CommandHelpers::appendCommandStatusNoThrow(*result,
                                                          response.getValue().writeConcernStatus);
    }

    CommandHelpers::filterCommandReplyForPassthrough(response.getValue().response, result);
    return true;
}

bool ShardingCatalogClientHost::runUserManagementReadCommand(OperationContext* opCtx,
                                                             const std::string& dbname,
                                                             const BSONObj& cmdObj,
                                                             BSONObjBuilder* result) {
    auto resultStatus =
        Grid::get(opCtx)->shardRegistry()->getHostShard()->runCommandWithFixedRetryAttempts(
            opCtx,
            kConfigPrimaryPreferredSelector,
            dbname,
            cmdObj,
            Shard::kDefaultConfigCommandTimeout,
            Shard::RetryPolicy::kIdempotent);
    if (resultStatus.isOK()) {
        CommandHelpers::filterCommandReplyForPassthrough(resultStatus.getValue().response, result);
        return resultStatus.getValue().commandStatus.isOK();
    }

    return CommandHelpers::appendCommandStatusNoThrow(*result, resultStatus.getStatus());  // XXX
}

Status ShardingCatalogClientHost::applyChunkOpsDeprecated(OperationContext* opCtx,
                                                          const BSONArray& updateOps,
                                                          const BSONArray& preCondition,
                                                          const NamespaceString& nss,
                                                          const ChunkVersion& lastChunkVersion,
                                                          const WriteConcernOptions& writeConcern,
                                                          repl::ReadConcernLevel readConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

Status ShardingCatalogClientHost::logAction(OperationContext* opCtx,
                                            const std::string& what,
                                            const std::string& ns,
                                            const BSONObj& detail) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

Status ShardingCatalogClientHost::logChange(OperationContext* opCtx,
                                            const std::string& what,
                                            const std::string& ns,
                                            const BSONObj& detail,
                                            const WriteConcernOptions& writeConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<BSONObj> ShardingCatalogClientHost::getGlobalSettings(OperationContext* opCtx,
                                                                 StringData key) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<VersionType> ShardingCatalogClientHost::getConfigVersion(
    OperationContext* opCtx, repl::ReadConcernLevel readConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

void ShardingCatalogClientHost::writeConfigServerDirect(OperationContext* opCtx,
                                                        const BatchedCommandRequest& request,
                                                        BatchedCommandResponse* response) {}

Status ShardingCatalogClientHost::insertConfigDocument(OperationContext* opCtx,
                                                       const NamespaceString& nss,
                                                       const BSONObj& doc,
                                                       const WriteConcernOptions& writeConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

void ShardingCatalogClientHost::insertConfigDocumentsAsRetryableWrite(
    OperationContext* opCtx,
    const NamespaceString& nss,
    std::vector<BSONObj> docs,
    const WriteConcernOptions& writeConcern) {}

StatusWith<bool> ShardingCatalogClientHost::updateConfigDocument(
    OperationContext* opCtx,
    const NamespaceString& nss,
    const BSONObj& query,
    const BSONObj& update,
    bool upsert,
    const WriteConcernOptions& writeConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

Status ShardingCatalogClientHost::removeConfigDocuments(OperationContext* opCtx,
                                                        const NamespaceString& nss,
                                                        const BSONObj& query,
                                                        const WriteConcernOptions& writeConcern) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

Status ShardingCatalogClientHost::createDatabase(OperationContext* opCtx,
                                                 const std::string& dbName) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

DistLockManager* ShardingCatalogClientHost::getDistLockManager() {
    return _distLockManager.get();
}

StatusWith<std::vector<KeysCollectionDocument>> ShardingCatalogClientHost::getNewKeys(
    OperationContext* opCtx,
    StringData purpose,
    const LogicalTime& newerThanThis,
    repl::ReadConcernLevel readConcernLevel) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

StatusWith<repl::OpTimeWith<std::vector<BSONObj>>>
ShardingCatalogClientHost::_exhaustiveFindOnConfig(OperationContext* opCtx,
                                                   const ReadPreferenceSetting& readPref,
                                                   const repl::ReadConcernLevel& readConcern,
                                                   const NamespaceString& nss,
                                                   const BSONObj& query,
                                                   const BSONObj& sort,
                                                   boost::optional<long long> limit) {
    return {ErrorCodes::InternalError, "Method not implemented"};
}

}  // namespace mongo
