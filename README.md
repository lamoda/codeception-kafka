# Codeception Kafka Extension

## THIS MODULE IS NOT PRODUCTION READY

This extension supports working with Apache Kafka.

## Installation

1. Install library
    ```bash
    composer require lamoda/codeception-kafka
    ```
   
2. Create message serializer for your data transfer object

```
namespace Tests\KafkaModule;

use App\EventBus\DtoInterface;
use Lamoda\Codeception\Extension\MessageSerializer\MessageSerializerInterface;

class AcmeMessageSerializer implements MessageSerializerInterface
{
    public function serialize($dto): string
    {
        if (!$dto instanceif DtoInterface) {
            throw new \RuntimeException('This value must be an ' . DtoInterface::class);
        }

        $message = json_encode($dto->toArray());

        if (!is_string($message)) {
            throw new \RuntimeException(json_last_error(), json_last_error_msg());
        }

        return $message;
    }
}
```

The default message serializer is Lamoda\Codeception\Extension\MessageSerializer\ArrayMessageSerializer.

2. Include to suite and configure
    ```yaml
    modules:
        enabled:
            - \Lamoda\Codeception\Extension\KafkaModule
                 serializer: 'Tests\KafkaModule\AcmeMessageSerializer'
                 config:
                     metadata.broker.list: '192.168.99.100:9092'
                     group.id: 'group_for_tests'
                 topic_config:
                     offset.store.sync.interval.ms: '0'
                     auto.commit.interval.ms: '500'
                     auto.offset.reset: 'smallest'   
    ```

## Development

### PHP Coding Standards Fixer

```bash
make php-cs-check
make php-cs-fix
```

### Tests

Unit

```bash
make test-unit
```
