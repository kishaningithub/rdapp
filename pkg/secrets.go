package rdapp

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	secretmanagertypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
)

type Secrets []secretmanagertypes.SecretListEntry

func (secrets Secrets) GetSecretArns() []string {
	var secretArns []string
	for _, secret := range secrets {
		secretArns = append(secretArns, *secret.ARN)
	}
	return secretArns
}

type SecretsService interface {
	FetchSecrets(ctx context.Context) (Secrets, error)
}

type secretService struct {
	secretsManagerClient SecretsManagerClient
}

func NewSecretsService(secretsManagerClient SecretsManagerClient) SecretsService {
	return &secretService{
		secretsManagerClient: secretsManagerClient,
	}
}

func (service *secretService) FetchSecrets(ctx context.Context) (Secrets, error) {
	var secrets Secrets
	secretsPaginator := secretsmanager.NewListSecretsPaginator(service.secretsManagerClient, nil)
	for secretsPaginator.HasMorePages() {
		listSecretsOutput, err := secretsPaginator.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("error occurred while fetching secrets from secrets manager: %w", err)
		}
		secrets = append(secrets, listSecretsOutput.SecretList...)
	}
	return secrets, nil
}
