<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\Database;
use Hamlet\Database\DatabaseException;
use Hamlet\Database\MultiProcedureContext;
use Hamlet\Database\Procedure;
use Hamlet\Database\Session;
use Hamlet\Database\SimpleConnectionPool;
use Hamlet\Database\SimpleMultiProcedureContext;
use mysqli;

/**
 * @extends Database<mysqli>
 */
class MySQLDatabase extends Database
{
    public function __construct(string $host, string $user, string $password, string $databaseName = null)
    {
        parent::__construct(new SimpleConnectionPool(
            function () use ($host, $user, $password, $databaseName): mysqli {
                if ($databaseName) {
                    return new mysqli($host, $user, $password, $databaseName);
                } else {
                    return new mysqli($host, $user, $password);
                }
            }
        ));
    }

    /**
     * @param mysqli $handle
     * @return Session
     * @psalm-suppress LessSpecificImplementedReturnType
     */
    protected function createSession($handle): Session
    {
        $session = new MySQLSession($handle);
        $session->setLogger($this->logger);
        return $session;
    }

    /**
     * @param array<callable(Session):Procedure> $generators
     * @return MultiProcedureContext
     */
    public function prepareMultiple(array $generators)
    {
        return new MySQLMultiProcedureContext($this->pool, $generators);
    }


    public static function exception(mysqli $connection): DatabaseException
    {
        return new DatabaseException($connection->error, $connection->errno);
    }
}
