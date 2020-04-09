<?php

declare(strict_types=1);

namespace Lamoda\Codeception\Extension;

use Codeception\Module;
use Exception;
use Lamoda\Codeception\Extension\MessageSerializer\MessageSerializerInterface;
use RdKafka\Conf;
use RdKafka\Consumer;
use RdKafka\Message;
use RdKafka\Producer;
use RdKafka\Queue;
use RdKafka\TopicConf;

class KafkaModule extends Module
{
    protected const DEFAULT_PARTITION = 0;

    /**
     * @var MessageSerializerInterface
     */
    protected $messageSerializer;

    /**
     * @var Conf
     */
    protected $conf;

    /**
     * @var TopicConf
     */
    protected $topicConf;

    /**
     * @var Consumer
     */
    protected $consumer;

    /**
     * @var Queue
     */
    protected $queue;

    /**
     * @param array $settings
     */
    public function _beforeSuite($settings = []): void
    {
        parent::_beforeSuite();

        if (isset($this->config['serializer']) && class_exists($this->config['serializer'])) {
            $this->messageSerializer = new $this->config['serializer']();
        } else {
            $this->messageSerializer = new ArrayMessageSerializer();
        }

        $this->conf = new Conf();

        if (isset($this->config['config']) && is_array($this->config['config'])) {
            foreach ($this->config['config'] as $key => $value) {
                $this->conf->set($key, $value);
            }
        }

        $this->topicConf = new TopicConf();

        if (isset($this->config['topic_config']) && is_array($this->config['topic_config'])) {
            foreach ($this->config['topic_config'] as $key => $value) {
                $this->topicConf->set($key, $value);
            }
        }

        $this->consumer = new Consumer($this->conf);
        $this->queue = $this->consumer->newQueue();
    }

    public function putMessageInTopic(string $topicName, string $message, ?int $partition = null): void
    {
        $producer = new Producer($this->conf);

        $topic = $producer->newTopic($topicName, $this->topicConf);

        $topic->produce($partition ?? static::DEFAULT_PARTITION, 0, $message);
    }

    public function putMessageListInTopic(string $topicName, array $messages, ?int $partition = null): void
    {
        foreach ($messages as $message) {
            $this->putMessageInTopic($topicName, $message, $partition);
        }
    }

    /**
     * @throws Exception
     */
    public function seeMessageInTopic(string $topicName, string $message, ?int $partition = null): void
    {
        $topMessage = $this->readOneMessageByCurrentOffset($topicName, $partition ?? static::DEFAULT_PARTITION);

        $this->assertNotNull($topMessage);
        $this->assertEquals($message, $topMessage->payload);
    }

    /**
     * @throws Exception
     */
    public function readAllMessagesFromTopic(string $topicName, ?int $partition = null, ?string $groupId = null): void
    {
        $topMessage = true;

        while (null !== $topMessage) {
            $topMessage = $this->readOneMessageByCurrentOffset(
                $topicName,
                $partition ?? static::DEFAULT_PARTITION,
                $groupId
            );
        }
    }

    /**
     * @param mixed $dto
     */
    public function putDtoInTopic(string $topicName, $dto, ?int $partition = null): void
    {
        $message = $this->messageSerializer->serialize($dto);
        $this->putMessageInTopic($topicName, $message, $partition);
    }

    /**
     * @param mixed $dto
     *
     * @throws Exception
     */
    public function seeDtoInTopic(string $topicName, $dto, ?int $partition = null): void
    {
        $message = $this->messageSerializer->serialize($dto);
        $this->seeMessageInTopic($topicName, $message, $partition);
    }

    /**
     * @throws Exception
     */
    public function assertTopicNotContainsUnreadMessages(string $topicName, ?int $partition = null): void
    {
        $this->assertNull($this->readOneMessageByCurrentOffset($topicName, $partition ?? static::DEFAULT_PARTITION));
    }

    /**
     * @throws Exception
     */
    private function readOneMessageByCurrentOffset(string $topicName, int $partition, ?string $groupId = null): ?Message
    {
        if (null === $groupId) {
            $consumer = $this->consumer;
            $queue = $this->queue;
        } else {
            $conf = new Conf();
            foreach ($this->config['config'] as $key => $value) {
                $conf->set($key, $value);
            }
            $conf->set('group.id', $groupId);
            $consumer = new Consumer($conf);
            $queue = $consumer->newQueue();
        }

        $topic = $consumer->newTopic($topicName, $this->topicConf);
        $topic->consumeQueueStart($partition, RD_KAFKA_OFFSET_STORED, $queue);

        $message = $queue->consume(2000);

        $topic->consumeStop($partition);

        return $this->decideUponMessage($message);
    }

    /**
     * @throws Exception
     */
    private function decideUponMessage(?Message $message = null): ?Message
    {
        if (!($message instanceof Message)) {
            return null;
        }

        switch ($message->err) {
            case RD_KAFKA_RESP_ERR_NO_ERROR:
                return $message;
                break;
            case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                return null;
                break;
            case RD_KAFKA_RESP_ERR__TIMED_OUT:
                throw new Exception('Timed out');
                break;
            default:
                throw new Exception($message->errstr(), $message->err);
                break;
        }
    }
}
