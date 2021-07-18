<?php

namespace milind\LaravelPubSub;

use Illuminate\Contracts\Container\Container;
use InvalidArgumentException;
use milind\PubSub\Adapters\DevNullPubSubAdapter;
use milind\PubSub\Adapters\LocalPubSubAdapter;
use milind\PubSub\GoogleCloud\GoogleCloudPubSubAdapter;
use milind\PubSub\HTTP\HTTPPubSubAdapter;
use milind\PubSub\Kafka\KafkaPubSubAdapter;
use milind\PubSub\Kafka\KafkaProducerAdapter;
use milind\PubSub\Kafka\KafkaHighLevelConsumerAdapter;
use milind\PubSub\Kafka\KafkaLowLevelConsumerAdapter;
use milind\PubSub\PubSubAdapterInterface;
use milind\PubSub\Redis\RedisPubSubAdapter;
use Illuminate\Support\Arr;

class PubSubConnectionFactory {

    /**
     * @var Container
     */
    protected $container;

    /**
     * @param Container $container
     */
    public function __construct(Container $container) {
        $this->container = $container;
    }

    /**
     * Factory a PubSubAdapterInterface.
     *
     * @param string $driver
     * @param array $config
     *
     * @return PubSubAdapterInterface
     */
    public function make($driver, array $config = []) {
        switch ($driver) {
            case '/dev/null':
                return new DevNullPubSubAdapter();
            case 'local':
                return new LocalPubSubAdapter();
            case 'redis':
                return $this->makeRedisAdapter($config);
            case 'kafka':
                return $this->makeKafkaAdapter($config);
            case 'kafkaProducer':
                return $this->makeKafkaProducerAdapter($config);
            case 'kafkaLowLevelConsumer':
                return $this->makeKafkaLowLevelConsumerAdapter($config);
            case 'kafkaHighLevelConsumer':
                return $this->makeKafkaHighLevelConsumerAdapter($config);
            case 'gcloud':
                return $this->makeGoogleCloudAdapter($config);
            case 'http':
                return $this->makeHTTPAdapter($config);
        }

        throw new InvalidArgumentException(sprintf('The driver [%s] is not supported.', $driver));
    }

    /**
     * Factory a RedisPubSubAdapter.
     *
     * @param array $config
     *
     * @return RedisPubSubAdapter
     */
    protected function makeRedisAdapter(array $config) {
        if (!isset($config['read_write_timeout'])) {
            $config['read_write_timeout'] = 0;
        }

        $client = $this->container->makeWith('pubsub.redis.redis_client', ['config' => $config]);

        return new RedisPubSubAdapter($client);
    }

    /**
     * Factory a KafkaPubSubAdapter.
     *
     * @param array $config
     *
     * @return KafkaPubSubAdapter
     */
    protected function makeKafkaAdapter(array $config) {
        // create producer

        $producerConf = $this->container->makeWith('pubsub.kafka.conf');
        $consumerConf = $this->container->makeWith('pubsub.kafka.conf');

        $producerDefaultconf = [
            "bootstrap.servers" => $config['brokers']
        ];
        $consumerDefaultconf = [
            "enable.auto.commit" => 'false',
            "auto.offset.reset" => 'smallest',
            "bootstrap.servers" => $config['brokers'],
            "group.id" => Arr::get($config, 'consumer_group_id', 'php-pubsub'),
        ];

        foreach (array_merge($consumerDefaultconf, Arr::get($config, 'consumerConfig', [])) as $key => $value) {
            $consumerConf->set($key, $value);
        }
        foreach (array_merge($producerDefaultconf, Arr::get($config, 'producerConfig', [])) as $key => $value) {
            $producerConf->set($key, $value);
        }

        $producer = $this->container->makeWith('pubsub.kafka.producer', ['conf' => $producerConf]);
        $producer->addBrokers($config['brokers']);

        $consumer = $this->container->makeWith('pubsub.kafka.consumer', ['conf' => $consumerConf]);

        return new KafkaPubSubAdapter($producer, $consumer);
    }

    /**
     * Factory a KafkaPublishAdapter.
     *
     * @param array $config
     *
     * @return KafkaPublishAdapter
     */
    protected function makeKafkaProducerAdapter(array $config) {
        // create producer

        $producerConf = $this->container->makeWith('pubsub.kafka.conf');

        $producerDefaultconf = [
            "bootstrap.servers" => $config['brokers']
        ];

        foreach (array_merge($producerDefaultconf, Arr::get($config, 'producerConfig', [])) as $key => $value) {
            $producerConf->set($key, $value);
        }

        $producer = $this->container->makeWith('pubsub.kafka.producer', ['conf' => $producerConf]);
        $producer->addBrokers($config['brokers']);

        return new KafkaProducerAdapter($producer);
    }

    /**
     * Factory a KafkaHighLevelConsumerAdapter.
     *
     * @param array $config
     *
     * @return KafkaHighLevelConsumerAdapter
     */
    protected function makeKafkaHighLevelConsumerAdapter(array $config) {
        // create producer

        $consumerConf = $this->container->makeWith('pubsub.kafka.conf');

        $consumerDefaultconf = [
            "enable.auto.commit" => 'false',
            "auto.offset.reset" => 'smallest',
            "bootstrap.servers" => $config['brokers'],
            "group.id" => Arr::get($config, 'consumer_group_id', 'php-pubsub'),
        ];

        foreach (array_merge($consumerDefaultconf, Arr::get($config, 'consumerConfig', [])) as $key => $value) {
            $consumerConf->set($key, $value);
        }

        $consumer = $this->container->makeWith('pubsub.kafka.consumer', ['conf' => $consumerConf]);

        return new KafkaHighLevelConsumerAdapter($consumer);
    }

    /**
     * Factory a KafkaLowLevelConsumerAdapter.
     *
     * @param array $config
     *
     * @return KafkaLowLevelConsumerAdapter
     */
    protected function makeKafkaLowLevelConsumerAdapter(array $config) {
        // create producer

        $consumerConf = $this->container->makeWith('pubsub.kafka.conf');

        $consumerDefaultconf = [
            "enable.auto.commit" => 'false',
            "auto.offset.reset" => 'smallest',
            "bootstrap.servers" => $config['brokers'],
            "group.id" => Arr::get($config, 'consumer_group_id', 'php-pubsub'),
        ];

        foreach (array_merge($consumerDefaultconf, Arr::get($config, 'consumerConfig', [])) as $key => $value) {
            $consumerConf->set($key, $value);
        }

        $consumer = $this->container->makeWith('pubsub.kafka.consumer', ['conf' => $consumerConf]);

        return new KafkaLowLevelConsumerAdapter($consumer);
    }

    /**
     * Factory a GoogleCloudPubSubAdapter.
     *
     * @param array $config
     *
     * @return GoogleCloudPubSubAdapter
     */
    protected function makeGoogleCloudAdapter(array $config) {
        $clientConfig = [
            'projectId' => $config['project_id'],
            'keyFilePath' => $config['key_file'],
        ];
        if (isset($config['auth_cache'])) {
            $clientConfig['authCache'] = $this->container->make($config['auth_cache']);
        }

        $client = $this->container->makeWith('pubsub.gcloud.pub_sub_client', ['config' => $clientConfig]);

        $clientIdentifier = Arr::get($config, 'client_identifier');
        $autoCreateTopics = Arr::get($config, 'auto_create_topics', true);
        $autoCreateSubscriptions = Arr::get($config, 'auto_create_subscriptions', true);
        $backgroundBatching = Arr::get($config, 'background_batching', false);
        $backgroundDaemon = Arr::get($config, 'background_daemon', false);

        if ($backgroundDaemon) {
            putenv('IS_BATCH_DAEMON_RUNNING=true');
        }
        return new GoogleCloudPubSubAdapter(
                $client, $clientIdentifier, $autoCreateTopics, $autoCreateSubscriptions, $backgroundBatching
        );
    }

    /**
     * Factory a HTTPPubSubAdapter.
     *
     * @param array $config
     *
     * @return HTTPPubSubAdapter
     */
    protected function makeHTTPAdapter(array $config) {
        $client = $this->container->make('pubsub.http.client');
        $adapter = $this->make(
                $config['subscribe_connection_config']['driver'], $config['subscribe_connection_config']
        );
        return new HTTPPubSubAdapter($client, $config['uri'], $adapter);
    }

}
