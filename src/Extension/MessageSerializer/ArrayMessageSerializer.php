<?php

declare(strict_types=1);

namespace Lamoda\Codeception\Extension\MessageSerializer;

class ArrayMessageSerializer implements MessageSerializerInterface
{
    public function serialize($dto): string
    {
        if (!is_array($dto)) {
            throw new \RuntimeException('This value must be an array');
        }

        $message = json_encode($dto);

        if (!is_string($message)) {
            throw new \RuntimeException(json_last_error(), json_last_error_msg());
        }

        return $message;
    }
}
