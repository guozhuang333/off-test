package offchain_transmission

type OffChainTransmission interface {
	Start() error

	Stop() error
}
