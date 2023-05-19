package main

import (
	"context"
	"fmt"
	"github.com/AlecAivazis/survey/v2"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	redshiftserverlesstypes "github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	"github.com/kishaningithub/rdapp/pkg"
)

type ConfigInstance struct {
	instanceName    string
	instanceDetails rdapp.RedshiftDataAPIConfig
}

type configInstances []ConfigInstance

func (instances configInstances) getInstanceNames() []string {
	var instanceNames []string
	for _, instance := range instances {
		instanceNames = append(instanceNames, instance.instanceName)
	}
	return instanceNames
}

func (instances configInstances) getInstanceByName(instanceName string) (ConfigInstance, error) {
	for _, instance := range instances {
		if instance.instanceName == instanceName {
			return instance, nil
		}
	}
	return ConfigInstance{}, fmt.Errorf("configuration instance is not found instanceName=%s", instanceName)
}

type InteractionService interface {
	Interact(ctx context.Context) (rdapp.RedshiftDataAPIConfig, error)
}

type interactionService struct {
	redshiftService rdapp.RedshiftService
	secretsService  rdapp.SecretsService
}

func NewInteractionService(redshiftService rdapp.RedshiftService, secretsService rdapp.SecretsService) InteractionService {
	return &interactionService{
		redshiftService: redshiftService,
		secretsService:  secretsService,
	}
}

func (service *interactionService) Interact(ctx context.Context) (rdapp.RedshiftDataAPIConfig, error) {
	instances, err := service.loadRedshiftConfigInstances(ctx)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	if len(instances) == 0 {
		return rdapp.RedshiftDataAPIConfig{}, fmt.Errorf("no redshift instances found try changing the aws region")
	}
	selectedInstance, err := service.selectInstance(instances)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	var useSecretManager bool
	err = survey.AskOne(&survey.Confirm{
		Message: "Would you like to choose a secret for connecting?",
	}, &useSecretManager)
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	if useSecretManager {
		secrets, err := service.secretsService.FetchSecrets(ctx)
		if err != nil {
			return rdapp.RedshiftDataAPIConfig{}, err
		}
		var selectedSecretArn string
		secretArns := secrets.GetSecretArns()
		if len(secretArns) == 0 {
			return rdapp.RedshiftDataAPIConfig{}, fmt.Errorf("no secrets found try changing the aws region")
		}
		err = survey.AskOne(&survey.Select{
			Message: "Choose the secret",
			Options: secretArns,
		}, &selectedSecretArn)
		if err != nil {
			return rdapp.RedshiftDataAPIConfig{}, err
		}
		selectedInstance.instanceDetails.SecretArn = &selectedSecretArn
		selectedInstance.instanceDetails.DbUser = nil
	}
	return selectedInstance.instanceDetails, nil
}

func (service *interactionService) selectInstance(instances configInstances) (ConfigInstance, error) {
	var selectedInstanceName string
	errMsg := "error while selecting config instance"
	err := survey.AskOne(&survey.Select{
		Message: "Which instance you want to connect to?",
		Options: instances.getInstanceNames(),
	}, &selectedInstanceName)
	if err != nil {
		return ConfigInstance{}, fmt.Errorf("%s: %w", errMsg, err)
	}
	selectedInstance, err := instances.getInstanceByName(selectedInstanceName)
	if err != nil {
		return ConfigInstance{}, fmt.Errorf("%s: %w", errMsg, err)
	}
	return selectedInstance, nil
}

func (service *interactionService) loadRedshiftConfigInstances(ctx context.Context) (configInstances, error) {
	provisionedClusters, err := service.redshiftService.FetchProvisionedClusters(ctx)
	if err != nil {
		return nil, err
	}
	serverlessNamespaces, err := service.redshiftService.FetchServerlessNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	workgroups, err := service.redshiftService.FetchServerlessWorkGroups(ctx)
	if err != nil {
		return nil, err
	}
	instances, err := service.computeConfigInstances(provisionedClusters, workgroups, serverlessNamespaces)
	if err != nil {
		return nil, err
	}
	return instances, nil
}

func (service *interactionService) computeConfigInstances(provisionedClusters []types.Cluster, serverlessWorkgroups []redshiftserverlesstypes.Workgroup, serverlessNamespaces []redshiftserverlesstypes.Namespace) (configInstances, error) {
	var instances configInstances
	provisionedClusterInstances := service.getProvisionedClusterInstances(provisionedClusters)
	serverlessClusterInstances, err := service.getServerlessInstances(serverlessWorkgroups, serverlessNamespaces)
	if err != nil {
		return instances, err
	}
	instances = append(instances, provisionedClusterInstances...)
	instances = append(instances, serverlessClusterInstances...)
	return instances, nil
}

func (service *interactionService) getServerlessInstances(serverlessWorkgroups []redshiftserverlesstypes.Workgroup, serverlessNamespaces []redshiftserverlesstypes.Namespace) (configInstances, error) {
	var instances configInstances
	for _, workgroup := range serverlessWorkgroups {
		if workgroup.Status != redshiftserverlesstypes.WorkgroupStatusAvailable {
			continue
		}
		namespace, err := service.findNameSpaceForWorkGroup(workgroup, serverlessNamespaces)
		if err != nil {
			return nil, err
		}
		instances = append(instances, ConfigInstance{
			instanceName: *workgroup.WorkgroupName,
			instanceDetails: rdapp.RedshiftDataAPIConfig{
				Database:      namespace.DbName,
				WorkgroupName: workgroup.WorkgroupName,
			},
		})
	}
	return instances, nil
}

func (service *interactionService) getProvisionedClusterInstances(provisionedClusters []types.Cluster) configInstances {
	var instances configInstances
	for _, cluster := range provisionedClusters {
		if *cluster.ClusterStatus != "available" {
			continue
		}
		instances = append(instances, ConfigInstance{
			instanceName: *cluster.ClusterIdentifier,
			instanceDetails: rdapp.RedshiftDataAPIConfig{
				Database:          cluster.DBName,
				ClusterIdentifier: cluster.ClusterIdentifier,
				DbUser:            cluster.MasterUsername,
			},
		})
	}
	return instances
}

func (service *interactionService) findNameSpaceForWorkGroup(workgroup redshiftserverlesstypes.Workgroup, serverlessNamespaces []redshiftserverlesstypes.Namespace) (redshiftserverlesstypes.Namespace, error) {
	var namespaces []string
	for _, namespace := range serverlessNamespaces {
		if *namespace.NamespaceName == *workgroup.NamespaceName {
			return namespace, nil
		}
		namespaces = append(namespaces, *namespace.NamespaceName)
	}
	return redshiftserverlesstypes.Namespace{}, fmt.Errorf("namespace not found for workgroup workgroup=%s requiredNamespace=%s availableNamespaces=%v", *workgroup.WorkgroupName, *workgroup.NamespaceName, namespaces)
}
