<?php

namespace Hamlet\Database\MySQL;

use Hamlet\Database\Database;
use Hamlet\Database\MySQL\MySQLDatabase;
use Hamlet\Database\Procedure;
use PHPUnit\Framework\Assert;
use PHPUnit\Framework\TestCase;

class MySQLDatabaseTest extends TestCase
{
    /** @var Database */
    private $database;

    /** @var Procedure */
    private $procedure;

    /** @var int */
    private $userId;

    public function setUp()
    {
        $this->database = new MySQLDatabase('0.0.0.0', 'root', '123456', 'test');

        $procedure = $this->database->prepare("INSERT INTO users (name) VALUES ('Vladimir')");
        $this->userId = $procedure->insert();

        $procedure = $this->database->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Moskva')");
        $procedure->bindInteger($this->userId);
        $procedure->execute();

        $procedure = $this->database->prepare("INSERT INTO addresses (user_id, address) VALUES (?, 'Vladivostok')");
        $procedure->bindInteger($this->userId);
        $procedure->execute();

        $this->procedure = $this->database->prepare('
            SELECT users.id,
                   name,
                   address
              FROM users 
                   JOIN addresses
                     ON users.id = addresses.user_id      
        ');
    }

    public function tearDown()
    {
        $this->database->prepare('DELETE FROM addresses WHERE 1')->execute();
        $this->database->prepare('DELETE FROM users WHERE 1')->execute();
    }

    public function testProcessOne()
    {
        $result = $this->procedure->processOne()
            ->coalesceAll()
            ->collectAll();

        Assert::assertEquals([$this->userId], $result);
    }

    public function testProcessAll()
    {
        $result = $this->procedure->processAll()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->collectAll();

        Assert::assertCount(1, $result);
        Assert::assertArrayHasKey($this->userId, $result);
        Assert::assertEquals('Vladimir', $result[$this->userId]['name']);
        Assert::assertCount(2, $result[$this->userId]['addresses']);
    }

    public function testFetchOne()
    {
        Assert::assertEquals(['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'], $this->procedure->fetchOne());
    }

    public function testFetchAll()
    {
        Assert::assertEquals([
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Moskva'],
            ['id' => $this->userId, 'name' => 'Vladimir', 'address' => 'Vladivostok']
        ], $this->procedure->fetchAll());
    }

    public function testStream()
    {
        $iterator = $this->procedure->stream()
            ->selectValue('address')->groupInto('addresses')
            ->selectFields('name', 'addresses')->name('user')
            ->map('id', 'user')->flatten()
            ->iterator();

        foreach ($iterator as $id => $user) {
            Assert::assertEquals($this->userId, $id);
            Assert::assertEquals(['Moskva', 'Vladivostok'], $user['addresses']);
        }
    }

    public function testInsert()
    {
        $procedure = $this->database->prepare("INSERT INTO users (name) VALUES ('Anatoly')");
        Assert::assertGreaterThan($this->userId, $procedure->insert());
    }
}
