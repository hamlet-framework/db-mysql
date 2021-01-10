<?php

namespace Hamlet\Database\MySQL;

use Generator;
use Hamlet\Database\Procedure;
use mysqli;
use RuntimeException;

class MySQLAsyncProcedure extends Procedure
{
    /**
     * @var mysqli
     */
    private $handle;

    /**
     * @var string
     */
    private $query;

    /**
     * @var array|null
     */
    private $resultSet = null;

    /**
     * @var mixed
     */
    private $lastInsertId = null;

    /**
     * @var mixed
     */
    private $affectedRows = null;

    public function __construct(mysqli $handle, string $query)
    {
        $this->handle = $handle;
        $this->query = $query;
    }

    /**
     * @return void
     */
    public function sendAsyncQuery()
    {
        $this->handle->query($this->interpolateQuery(), MYSQLI_ASYNC);
    }

    public function reapAsyncQuery(): bool
    {
        $result = $this->handle->reap_async_query();
        if ($result) {
            $this->resultSet = [];
            while (($row = $result->fetch_assoc()) !== null) {
                $this->resultSet[] = $row;
            }
            $this->lastInsertId = $this->handle->insert_id;
            $this->affectedRows = $this->handle->affected_rows;
            return true;
        } else {
            return false;
        }
    }

    /**
     * @return Generator
     * @psalm-return Generator<int,array<string,int|string|float|null>,mixed,void>
     * @psalm-suppress MixedReturnTypeCoercion
     */
    protected function fetch(): Generator
    {
        yield from $this->resultSet ?? [];
    }

    public function insert(): int
    {
        return $this->lastInsertId ? (int) $this->lastInsertId : -1;
    }

    /**
     * @return void
     */
    public function execute()
    {
    }

    public function affectedRows(): int
    {
        return $this->affectedRows ? (int) $this->affectedRows : 0;
    }

    /**
     * @todo a lot of work here including caching
     * @return string
     */
    private function interpolateQuery(): string
    {
        $query = $this->query;
        foreach ($this->parameters as list($type, $value)) {
            $replacement = null;
            if (is_scalar($value)) {
                switch ($type) {
                    case 'i':
                        $replacement = (int) $value;
                        break;
                    case 'd':
                        $replacement = (float) $value;
                        break;
                    case 's':
                        $replacement = $this->handle->real_escape_string((string) $value);
                        break;
                }
            }
            if ($replacement === null) {
                throw new RuntimeException('Unsupported parameter type');
            }
            $position = strpos($query, '?');
            if ($position === false) {
                throw new RuntimeException('Invalid number of arguments');
            }
            $query = substr_replace($query, (string) $replacement, $position, 1);
        }
        return $query;
    }
}
