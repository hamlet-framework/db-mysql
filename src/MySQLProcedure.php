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

    /**
     * @var mysqli
     */
    private $handle;

    /**
     * @var string
     */
    private $query;

    /**
     * @var mysqli_stmt[]
     * @psalm-var array<string,mysqli_stmt>
     */
    private $cache = [];

    public function __construct(mysqli $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    /**
     * @return void
     */
    public function execute()
    {
        $this->executeInternal($this->handle);
    }

    /**
     * @return int
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function insert(): int
    {
        $this->executeInternal($this->handle);
        return (int) $this->handle->insert_id;
    }

    /**
     * @return Generator
     * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
     * @psalm-suppress MixedInferredReturnType
     * @psalm-suppress MixedReturnStatement
     */
    public function fetch(): Generator
    {
        list($row, $statement) = $this->initFetching($this->handle);
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
                throw MySQLDatabase::exception($this->handle);
            }
        }
        $this->finalizeFetching($this->handle, $statement);
    }

    public function affectedRows(): int
    {
        return (int) ($this->handle->affected_rows ?? -1);
    }

    /**
     * @param mysqli $handle
     * @return void
     */
    private function executeInternal(mysqli $handle)
    {
        $statement = $this->bindParameters($handle);
        $executionSucceeded = $statement->execute();
        if ($executionSucceeded === false) {
            throw MySQLDatabase::exception($handle);
        }
    }

    /**
     * @param mysqli $handle
     * @return mysqli_stmt
     */
    private function bindParameters(mysqli $handle): mysqli_stmt
    {
        list($query, $parameters) = $this->unwrapQueryAndParameters($this->query, $this->parameters);
        $this->parameters = [];

        $statement = $this->cache[$query] = ($this->cache[$query] ?? $handle->prepare($query));
        if ($statement === false) {
            throw MySQLDatabase::exception($handle);
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
            throw MySQLDatabase::exception($handle);
        }

        return $statement;
    }

    /**
     * @param mysqli $handle
     * @return array
     * @psalm-return array{0:array<string,int|float|string|null>,1:mysqli_stmt}
     */
    private function initFetching(mysqli $handle): array
    {
        $statement = $this->bindParameters($handle);
        $row = $this->bindResult($handle, $statement);
        $success = $statement->execute();
        if (!$success) {
            throw MySQLDatabase::exception($handle);
        }
        $statement->store_result();
        return [$row, $statement];
    }

    /**
     * @param mysqli $handle
     * @param mysqli_stmt $statement
     * @return void
     */
    private function finalizeFetching(mysqli $handle, mysqli_stmt $statement)
    {
        $statement->free_result();
        $success = $statement->close();
        if (!$success) {
            throw MySQLDatabase::exception($handle);
        }
    }

    /**
     * @param mysqli $handle
     * @param mysqli_stmt $statement
     * @return array
     * @psalm-return array<string,int|float|string|null>
     */
    private function bindResult(mysqli $handle, mysqli_stmt $statement): array
    {
        $metaData = $statement->result_metadata();
        if ($metaData === false) {
            throw MySQLDatabase::exception($handle);
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
            throw MySQLDatabase::exception($handle);
        }
        return $row;
    }
}
