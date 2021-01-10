<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\ConnectionPool;
use Hamlet\Database\MultiProcedureContext;
use mysqli;

class MySQLMultiProcedureContext implements MultiProcedureContext
{
    /**
     * @var ConnectionPool<mysqli>
     */
    private $pool;

    /**
     * @var array<callable(\Hamlet\Database\Session):\Hamlet\Database\Procedure>
     */
    private $generators;

    /**
     * @param ConnectionPool<mysqli> $pool
     * @param array<callable(\Hamlet\Database\Session):\Hamlet\Database\Procedure> $generators
     */
    public function __construct(ConnectionPool $pool, array $generators)
    {
        $this->pool = $pool;
        $this->generators = $generators;
    }

    /**
     * @template T
     * @param callable(\Hamlet\Database\Procedure):T $processor
     * @return array<T>
     * @psalm-suppress MissingClosureReturnType
     */
    public function forEach(callable $processor): array
    {
        $generators = [];
        $result = [];
        $handles = [];
        $procedures = [];
        $keys = [];
        foreach ($this->generators as $key => &$generator) {
            $generators[] = [$key, $generator];
        }
        $completed = 0;
        while ($completed < count($this->generators)) {
            if ($generators) {
                list($key, $generator) = array_shift($generators);
                $handle = $this->pool->pop();
                $session = new MySQLAsyncSession($handle);
                $procedure = $generator($session);
                assert($procedure instanceof MySQLAsyncProcedure);
                $procedure->sendAsyncQuery();

                $handles[$key] = $handle;
                $procedures[$key] = $procedure;

                $keys[$handle->thread_id] = $key;
            }
            $links = $errors = $reject = [];
            foreach ($handles as $handle) {
                $links[] = $errors[] = $reject[] = $handle;
            }
            if (!mysqli_poll($links, $errors, $reject, 1)) {
                continue;
            }
            foreach ($links as $handle) {
                $key = $keys[$handle->thread_id];
                $procedure = $procedures[$key];
                if ($procedure->reapAsyncQuery()) {
                    $result[$key] = $processor($procedure);
                }
                unset($procedures[$key]);
                unset($handles[$key]);
                $completed++;
                $this->pool->push($handle);
            }
        }
        return $result;
    }
}
