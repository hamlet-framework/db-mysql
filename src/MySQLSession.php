<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use mysqli;

/**
 * @extends Session<mysql>
 */
class MySQLSession extends Session
{
    /**
     * @param mysqli $handle
     */
    public function __construct(mysqli $handle)
    {
        parent::__construct($handle);
    }

    /**
     * @param string $query
     * @return Procedure
     */
    public function prepare(string $query): Procedure
    {
        $procedure = new MySQLProcedure($this->handle, $query);
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
            throw MySQLDatabase::exception($connection);
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
            throw MySQLDatabase::exception($connection);
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
            throw MySQLDatabase::exception($connection);
        }
    }
}
