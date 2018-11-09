<?php

namespace Hamlet\Database\MySQL;

use Generator;
use Hamlet\Database\Procedure;
use Hamlet\Database\Traits\QueryExpanderTrait;
use mysqli;
use mysqli_stmt;

class MySQLProcedure extends Procedure
{
    use QueryExpanderTrait;

    /** @var callable */
    private $executor;

    /** @var string */
    private $query;

    /** @var mixed */
    private $lastInsertId;

    /** @var int|null */
    private $affectedRows;

    public function __construct(callable $executor, string $query)
    {
        $this->executor = $executor;
        $this->query = $query;
    }

    public function execute(): void
    {
        ($this->executor)(function (mysqli $connection) {
            $this->executeInternal($connection);
        });
    }

    public function insert(): int
    {
        return ($this->executor)(function (mysqli $connection) {
            $this->executeInternal($connection);
            return $this->lastInsertId;
        });
    }

    /**
     * @return Generator<int,array<string,int|string|float|null>>
     */
    public function fetch(): Generator
    {
        return ($this->executor)(function (mysqli $connection) {
            list($row, $statement) = $this->initFetching($connection);
            $index = 0;
            while (true) {
                $status = $statement->fetch();
                if ($status === true) {
                    $rowCopy = [];
                    foreach ($row as $key => $value) {
                        $rowCopy[$key] = $value;
                    }
                    yield $index++ => $rowCopy;
                } elseif ($status === null) {
                    break;
                } else {
                    throw MySQLDatabase::exception($connection);
                }
            }
            $this->finalizeFetching($connection, $statement);
        });
    }

    public function affectedRows(): int
    {
        return $this->affectedRows ?? -1;
    }

    /**
     * @param mysqli $connection
     * @return void
     */
    private function executeInternal(mysqli $connection)
    {
        $statement = $this->bindParameters($connection);
        $executionSucceeded = $statement->execute();
        if ($executionSucceeded === false) {
            throw MySQLDatabase::exception($connection);
        }
        $closeSucceeded = $statement->close();
        if (!$closeSucceeded) {
            throw MySQLDatabase::exception($connection);
        }
        $this->lastInsertId = $connection->insert_id;
        $this->affectedRows = (int) $connection->affected_rows;
    }

    /**
     * @param mysqli $connection
     * @return mysqli_stmt
     */
    private function bindParameters(mysqli $connection): mysqli_stmt
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $connection->prepare($query);
        if ($statement === false) {
            throw MySQLDatabase::exception($connection);
        }
        if (count($parameters) == 0) {
            return $statement;
        }

        $signature = '';
        $values = [];
        foreach ($parameters as list ($type, $value)) {
            $signature .= $type;
            $values[] = $value;
        }
        $success = $statement->bind_param($signature, ...$values);
        if (!$success) {
            throw MySQLDatabase::exception($connection);
        }

        return $statement;
    }

    /**
     * @param mysqli $connection
     * @return array
     * @psalm-return array{0:array<string,int|float|string|null>,1:mysqli_stmt}
     */
    private function initFetching(mysqli $connection): array
    {
        $statement = $this->bindParameters($connection);
        $row = $this->bindResult($connection, $statement);
        $success = $statement->execute();
        if (!$success) {
            throw MySQLDatabase::exception($connection);
        }
        $statement->store_result();
        return [$row, $statement];
    }

    /**
     * @param mysqli $connection
     * @param mysqli_stmt $statement
     * @return void
     */
    private function finalizeFetching(mysqli $connection, mysqli_stmt $statement)
    {
        $statement->free_result();
        $success = $statement->close();
        if (!$success) {
            throw MySQLDatabase::exception($connection);
        }
    }

    /**
     * @param mysqli $connection
     * @param mysqli_stmt $statement
     * @return array<string,int|float|string|null>
     */
    private function bindResult(mysqli $connection, mysqli_stmt $statement): array
    {
        $metaData = $statement->result_metadata();
        if ($metaData === false) {
            throw MySQLDatabase::exception($connection);
        }
        $row = [];
        $boundParameters = [];
        while ($field = $metaData->fetch_field()) {
            $name = (string) $field->name;
            $row[$name] = null;
            $boundParameters[] = &$row[$name];
        }
        $success = $statement->bind_result(...$boundParameters);
        if (!$success) {
            throw MySQLDatabase::exception($connection);
        }
        return $row;
    }
}
