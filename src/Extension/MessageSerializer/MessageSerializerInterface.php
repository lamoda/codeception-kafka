<?php

declare(strict_types=1);

namespace Lamoda\Codeception\Extension\MessageSerializer;

interface MessageSerializerInterface
{
    public function serialize($dto);
}
