package main

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/redshift"
	"github.com/aws/aws-sdk-go-v2/service/redshift/types"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftserverless"
	redshiftserverlesstypes "github.com/aws/aws-sdk-go-v2/service/redshiftserverless/types"
	tea "github.com/charmbracelet/bubbletea"
	rdapp "github.com/kishaningithub/rdapp/pkg"
	"strings"
)

type errMsg struct{ err error }

func NewErrMsg(err error) error {
	return errMsg{
		err: err,
	}
}

func (e errMsg) Error() string { return e.err.Error() }

var _ tea.Model = InteractionModel{}

type InteractionModel struct {
	redshiftClient           *redshift.Client
	redshiftServerlessClient *redshiftserverless.Client
	provisionedClusters      []types.Cluster
	serverlessWorkgroups     []redshiftserverlesstypes.Workgroup
	redshiftDataApiClient    *redshiftdata.Client
	redshiftDataApiConfig    rdapp.RedshiftDataAPIConfig
	ctx                      context.Context
	err                      error
}

func NewInteractionModel(ctx context.Context, redshiftClient *redshift.Client, redshiftServerlessClient *redshiftserverless.Client, redshiftDataApiClient *redshiftdata.Client) InteractionModel {
	return InteractionModel{
		ctx:                      ctx,
		redshiftClient:           redshiftClient,
		redshiftServerlessClient: redshiftServerlessClient,
		redshiftDataApiClient:    redshiftDataApiClient,
	}
}

func (interactionModel InteractionModel) Init() tea.Cmd {
	return func() tea.Msg {
		describeClustersPaginator := redshift.NewDescribeClustersPaginator(interactionModel.redshiftClient, nil)
		for describeClustersPaginator.HasMorePages() {
			page, err := describeClustersPaginator.NextPage(interactionModel.ctx)
			if err != nil {
				return NewErrMsg(err)
			}
			interactionModel.provisionedClusters = append(interactionModel.provisionedClusters, page.Clusters...)
		}
		listWorkgroupsPaginator := redshiftserverless.NewListWorkgroupsPaginator(interactionModel.redshiftServerlessClient, nil)
		for listWorkgroupsPaginator.HasMorePages() {
			page, err := listWorkgroupsPaginator.NextPage(interactionModel.ctx)
			if err != nil {
				return NewErrMsg(err)
			}
			interactionModel.serverlessWorkgroups = append(interactionModel.serverlessWorkgroups, page.Workgroups...)
		}
		return interactionModel
	}
}

func (interactionModel InteractionModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			return interactionModel, tea.Quit
		}
		if msg.Type == tea.KeyUp {

		}
	case errMsg:
		interactionModel.err = msg
		return interactionModel, tea.Quit
	case InteractionModel:
		return msg, nil
	}

	return interactionModel, nil
}

func (interactionModel InteractionModel) View() string {
	var view string
	view += fmt.Sprintln("Select redshift instance...")
	for _, cluster := range interactionModel.provisionedClusters {
		view += fmt.Sprintf("cluster %s | provisioned | %s\n", *cluster.ClusterIdentifier, *cluster.ClusterStatus)
	}
	for _, workgroup := range interactionModel.serverlessWorkgroups {
		view += fmt.Sprintf("%s | serverless | %s\n", *workgroup.WorkgroupName, strings.ToLower(string(workgroup.Status)))
	}
	return view
}

func getConfigFromInteractiveMode(ctx context.Context, redshiftClient *redshift.Client, redshiftServerlessClient *redshiftserverless.Client, redshiftDataApiClient *redshiftdata.Client) (rdapp.RedshiftDataAPIConfig, error) {
	program := tea.NewProgram(NewInteractionModel(ctx, redshiftClient, redshiftServerlessClient, redshiftDataApiClient))
	model, err := program.StartReturningModel()
	if err != nil {
		return rdapp.RedshiftDataAPIConfig{}, err
	}
	switch typedModel := model.(type) {
	case InteractionModel:
		return typedModel.redshiftDataApiConfig, nil
	}
	return rdapp.RedshiftDataAPIConfig{}, fmt.Errorf("invalid model type %+v", model)
}
