<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\ConnectionPool;
use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\Procedure;
use mysqli;

/**
 * @template-extends Database<T>
 */
class MySQLDatabase extends Database
{
    public function __construct(string $host, string $user, string $password, string $databaseName = null)
    {
        $connector = function () use ($host, $user, $password, $databaseName): mysqli {
            if ($databaseName) {
                return new mysqli($host, $user, $password, $databaseName);
            } else {
                return new mysqli($host, $user, $password);
            }
        };
        $pool = new ConnectionPool($connector);
        return parent::__construct($pool);
    }

    public function prepare(string $query): Procedure
    {
        $procedure = new MySQLProcedure($this->executor(), $query);
        $procedure->setLogger($this->logger);
        return $procedure;
    }

    /**
     * @param mysqli $connection
     * @return void
     */
    protected function startTransaction($connection)
    {
        $this->logger->debug('Starting transaction');
        $success = $connection->begin_transaction();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param mysqli $connection
     * @return void
     */
    protected function commit($connection)
    {
        $this->logger->debug('Committing transaction');
        $success = $connection->commit();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    /**
     * @param mysqli $connection
     * @return void
     */
    protected function rollback($connection)
    {
        $this->logger->debug('Rolling back transaction');
        $success = $connection->rollback();
        if (!$success) {
            throw self::exception($connection);
        }
    }

    public static function exception(mysqli $connection): DatabaseException
    {
        return new DatabaseException((string) ($connection->error ?? 'Unknown error'), (int) ($connection->errno ?? -1));
    }
}
