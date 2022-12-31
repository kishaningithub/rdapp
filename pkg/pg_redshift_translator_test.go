package rdapp

import (
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/require"
	"testing"
)

func Test_pgRedshiftTranslator_TranslateToRedshiftQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			name: "question mark params",
			args: args{query: "select * from person where name = ? and age > ?"},
			want: "select * from person where name = :1 and age > :2",
		},
		{
			name: "dollar params",
			args: args{query: "select * from person where name = $1 and age > $2"},
			want: "select * from person where name = :1 and age > :2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &pgRedshiftTranslator{}
			got := translator.TranslateToRedshiftQuery(tt.args.query)
			require.Equal(t, tt.want, got)
		})
	}
}

func Test_pgRedshiftTranslator_TranslateToRedshiftQueryParams(t *testing.T) {
	type args struct {
		pgParams []string
	}
	tests := []struct {
		name string
		args args
		want []types.SqlParameter
	}{
		{
			name: "translate args to sql position params",
			args: args{
				pgParams: []string{"name", "20"},
			},
			want: []types.SqlParameter{
				{Name: aws.String("1"), Value: aws.String("name")},
				{Name: aws.String("2"), Value: aws.String("20")},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &pgRedshiftTranslator{}
			got := translator.TranslateToRedshiftQueryParams(tt.args.pgParams)
			require.Equal(t, tt.want, got)
		})
	}
}
