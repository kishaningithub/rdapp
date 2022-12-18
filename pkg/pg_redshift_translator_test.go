package rdapp

import "testing"

func Test_pgRedshiftTranslator_TranslateToRedshiftQuery(t *testing.T) {
	type args struct {
		query string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"question mark params", args{query: "select * from person where name = ? and age > ?"}, "select * from person where name = :1 and age > :2"},
		{"dollar params", args{query: "select * from person where name = $1 and age > $2"}, "select * from person where name = :1 and age > :2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			translator := &pgRedshiftTranslator{}
			if got := translator.TranslateToRedshiftQuery(tt.args.query); got != tt.want {
				t.Errorf("TranslateToRedshiftQuery() = %v, want %v", got, tt.want)
			}
		})
	}
}
