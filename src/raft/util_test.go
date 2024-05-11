package raft

import "testing"

func TestLOG(t *testing.T) {
	type args struct {
		peerId int
		term   int
		topic  logTopic
		format string
		a      []interface{}
	}
	tests := []struct {
		name string
		args args
	}{
		{
			"测试输出日志",
			args{
				peerId: 1,
				term:   2,
				topic:  DError,
				format: "yjt",
				a:      nil,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			LOG(tt.args.peerId, tt.args.term, tt.args.topic, tt.args.format, tt.args.a...)
		})
	}
}
