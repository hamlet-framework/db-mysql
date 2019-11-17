<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\Procedure;

class MySQLAsyncSession extends MySQLSession
{
    /**
     * @param string $query
     * @return Procedure
     */
    public function prepare(string $query): Procedure
    {
        $procedure = new MySQLAsyncProcedure($this->handle, $query);
        $procedure->setLogger($this->logger);
        return $procedure;
    }
}
