package lotus

import (
	"context"
	"fmt"
	"net/http"

	"github.com/filecoin-project/go-jsonrpc"
	"github.com/filecoin-project/lotus/api/apistruct"
)

type ClientBuilder func(ctx context.Context) (*apistruct.FullNodeStruct, func(), error)

func NewBuilder(authToken string, addr string) (ClientBuilder, error) {
	headers := http.Header{
		"Authorization": []string{"Bearer " + authToken},
	}

	return func(ctx context.Context) (*apistruct.FullNodeStruct, func(), error) {
		var api apistruct.FullNodeStruct
		var closer jsonrpc.ClientCloser
		var err error
		closer, err = jsonrpc.NewMergeClient(context.Background(), "ws://"+addr+"/rpc/v0", "Filecoin",
			[]interface{}{
				&api.Internal,
				&api.CommonStruct.Internal,
			}, headers)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to connect to Lotus client %s", err)
		}

		return &api, closer, nil
	}, nil
}
