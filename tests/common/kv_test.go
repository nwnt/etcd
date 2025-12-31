// Copyright 2022 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package common

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.etcd.io/etcd/api/v3/mvccpb"
	"go.etcd.io/etcd/api/v3/v3rpc/rpctypes"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/tests/v3/framework/config"
	"go.etcd.io/etcd/tests/v3/framework/testutils"
)

func TestKVPut(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, tc := range clusterTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			clus := testRunner.NewCluster(ctx, t, config.WithClusterConfig(tc.config))
			defer clus.Close()
			cc := testutils.MustClient(clus.Client())

			testutils.ExecuteUntil(ctx, t, func() {
				key, value := "foo", "bar"

				_, err := cc.Put(ctx, key, value, config.PutOptions{})
				require.NoErrorf(t, err, "count not put key %q", key)
				resp, err := cc.Get(ctx, key, config.GetOptions{})
				require.NoErrorf(t, err, "count not get key %q, err: %s", key, err)
				assert.Lenf(t, resp.Kvs, 1, "Unexpected length of response, got %d", len(resp.Kvs))
				assert.Equalf(t, string(resp.Kvs[0].Key), key, "Unexpected key, want %q, got %q", key, resp.Kvs[0].Key)
				assert.Equalf(t, string(resp.Kvs[0].Value), value, "Unexpected value, want %q, got %q", value, resp.Kvs[0].Value)
			})
		})
	}
}

type kvbuilder mvccpb.KeyValue

func kv(key string, value ...string) *kvbuilder {
	kvb := &kvbuilder{
		Key: []byte(key),
	}
	if len(value) != 0 {
		kvb.Value = []byte(value[0])
	}
	return kvb
}

func (kv *kvbuilder) createRev(rev int) *kvbuilder {
	kv.CreateRevision = int64(rev)
	return kv
}

func (kv *kvbuilder) modRev(rev int) *kvbuilder {
	kv.ModRevision = int64(rev)
	return kv
}

func (kv *kvbuilder) version(ver int) *kvbuilder {
	kv.Version = int64(ver)
	return kv
}

func (kv *kvbuilder) build() *mvccpb.KeyValue {
	return (*mvccpb.KeyValue)(kv)
}

type rangeTestCase struct {
	description string
	key         string
	options     config.GetOptions

	wantResponse *clientv3.GetResponse
	wantError    error
}

func TestKVGet(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, tc := range clusterTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			clus := testRunner.NewCluster(ctx, t, config.WithClusterConfig(tc.config))
			defer clus.Close()
			cc := testutils.MustClient(clus.Client())

			testutils.ExecuteUntil(ctx, t, func() {
				puts := []testutils.KV{
					{Key: "a", Val: "fooa"},
					{Key: "x", Val: "foox"},
					{Key: "b", Val: "foob"},
					{Key: "y", Val: "fooy"},
					{Key: "c", Val: "fooc"},
					{Key: "z", Val: "fooz"},
					{Key: "d", Val: "food"},
					{Key: "e", Val: "fooe"},
					{Key: "f", Val: "foof"},
					{Key: "c/xyz", Val: "4ever"},
					{Key: "c", Val: "foocc"},
					{Key: "g", Val: "foog"},
					{Key: "c", Val: "fooccc"},
					{Key: "aa", Val: "bar"},
					{Key: "c/abc", Val: "egg"},
				}

				verify := func(t *testing.T, testcases []rangeTestCase) {
					for _, tt := range testcases {
						t.Run(tt.description, func(t *testing.T) {
							resp, err := cc.Get(ctx, tt.key, tt.options)
							if tt.wantError != nil {
								require.Contains(t, err.Error(), tt.wantError.Error())
								require.Nil(t, resp)
								return
							}
							require.NoErrorf(t, err, "count not get key %q, err: %s", tt.key, err)
							assert.Equal(t, tt.wantResponse.Kvs, resp.Kvs)
							assert.Equal(t, tt.wantResponse.Count, resp.Count)
							assert.Equal(t, tt.wantResponse.More, resp.More)
						})
					}
				}
				var firstRev int
				for i := range puts {
					resp, err := cc.Put(ctx, puts[i].Key, puts[i].Val, config.PutOptions{})
					require.NoErrorf(t, err, "could not put key %q", puts[i])
					if i == 0 {
						firstRev = int(resp.Header.Revision)
					}
				}
				baseTestCases := []rangeTestCase{
					{
						description: "no key with prefix and specific revision -> all KVs before the revision",
						key:         "",
						options: config.GetOptions{
							Prefix:   true,
							Revision: firstRev + 4,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 5,
							Kvs: []*mvccpb.KeyValue{
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c", "fooc").createRev(firstRev + 4).modRev(firstRev + 4).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - first update",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 10, // first update on 'c'
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "foocc").createRev(firstRev + 4).modRev(firstRev + 10).version(2).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - after first update but before second update",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 11,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "foocc").createRev(firstRev + 4).modRev(firstRev + 10).version(2).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - second update",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - after second update, but before current rev",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12 + 2, // 2 revs after 2nd update
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - current rev",
						key:         "c",
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
							},
							More: false,
						},
					},
					{
						description: "key range with an end and min/max mod revs -> range filtered by min/max mod revs, sorted descending",
						key:         "a",
						options: config.GetOptions{
							End:            "z",
							MinModRevision: firstRev + 1,
							MaxModRevision: firstRev + 5,
							Order:          clientv3.SortDescend,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 12,
							Kvs: []*mvccpb.KeyValue{
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "all keys with min/max create revs -> range filtered by min/max create revs",
						key:         "",
						options: config.GetOptions{
							Prefix:            true,
							MinCreateRevision: firstRev + 1,
							MaxCreateRevision: firstRev + 5,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "prefix of c",
						key:         "c",
						options: config.GetOptions{
							Prefix: true,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 3,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "from key c",
						key:         "c",
						options: config.GetOptions{
							FromKey: true,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 10,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "fromkey without any specified key -> return all",
						key:         "",
						options: config.GetOptions{
							FromKey: true,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "start and end covering all entries",
						key:         "a",
						options: config.GetOptions{
							End: "zz",
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "start and end covering all entries, but keys only",
						key:         "a",
						options: config.GetOptions{
							KeysOnly: true,
							End:      "zz",
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("a").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("aa").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("g").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("x").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
						},
					},
					{
						description: "start and end covering all entries, but count only",
						key:         "a",
						options: config.GetOptions{
							CountOnly: true,
							End:       "zz",
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs:   nil,
							More:  false,
						},
					},
					{
						description: "--count-only overrides --keys-only when both are true",
						key:         "a",
						options: config.GetOptions{
							KeysOnly:  true,
							CountOnly: true,
							End:       "zz",
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs:   nil,
							More:  false,
						},
					},
					{
						description: "from key of c, but with a limit",
						key:         "c",
						options: config.GetOptions{
							FromKey: true,
							Limit:   2,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 10,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
							},
							More: true,
						},
					},
					{
						description: "all entries, sorted by their mod revision",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
							Order:  clientv3.SortNone,
							SortBy: clientv3.SortByModRevision,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "all entries sorted by key descending with limit",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
							Limit:  3,
							Order:  clientv3.SortDescend,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
							},
							More: true,
						},
					},
					{
						description: "all entries sorted by key descending with a limit, and keys only",
						key:         "",
						options: config.GetOptions{
							Prefix:   true,
							Limit:    3,
							KeysOnly: true,
							Order:    clientv3.SortDescend,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("z").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("y").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("x").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
							},
							More: true,
						},
					},
					{
						description: "all entries, sorted by create revision",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
							Order:  clientv3.SortDescend,
							SortBy: clientv3.SortByCreateRevision,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "all entries, sorted by key descending",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
							Order:  clientv3.SortDescend,
							SortBy: clientv3.SortByKey,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
							},
							More: false,
						},
					},
					{
						description: "all entries, by default sorted by key descending",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
							Order:  clientv3.SortDescend,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
							},
							More: false,
						},
					},
				}
				verify(t, baseTestCases)

				var currentRevision int64
				keysToDeleted := []string{"a", "z", "g", "c"}
				for _, key := range keysToDeleted {
					resp, err := cc.Delete(ctx, key, config.DeleteOptions{})
					require.NoErrorf(t, err, "could not delete key %q", key)
					currentRevision = resp.Header.Revision
				}

				afterDeleteTestCases := []rangeTestCase{
					{
						description: "the keys are deleted from the current revision",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 9,
							Kvs: []*mvccpb.KeyValue{
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
							},
						},
					},
					{
						description: "if the entries are not compacted yet, they can still be retrieved by specifying the revision",
						key:         "",
						options: config.GetOptions{
							Prefix:   true,
							Revision: firstRev + 14,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 13,
							Kvs: []*mvccpb.KeyValue{
								kv("a", "fooa").createRev(firstRev).modRev(firstRev).version(1).build(),
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("g", "foog").createRev(firstRev + 11).modRev(firstRev + 11).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
								kv("z", "fooz").createRev(firstRev + 5).modRev(firstRev + 5).version(1).build(),
							},
						},
					},
					{
						description: "original version of the key is still available",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 4,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooc").createRev(firstRev + 4).modRev(firstRev + 4).version(1).build(),
							},
						},
					},
					{
						description: "a specific key with a specific revision - first update, still available after deleting",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 10, // first update on 'c'
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "foocc").createRev(firstRev + 4).modRev(firstRev + 10).version(2).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - after first update but before second update, still available",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 11,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "foocc").createRev(firstRev + 4).modRev(firstRev + 10).version(2).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - second update, still available",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - after second update, but before current rev, still available",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12 + 2, // 2 revs after 2nd update
						},
						wantResponse: &clientv3.GetResponse{
							Count: 1,
							Kvs: []*mvccpb.KeyValue{
								kv("c", "fooccc").createRev(firstRev + 4).modRev(firstRev + 12).version(3).build(),
							},
							More: false,
						},
					},
					{
						description: "a specific key with a specific revision - current rev, no longer available",
						key:         "c",
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
				}
				verify(t, afterDeleteTestCases)

				_, err := cc.Compact(ctx, currentRevision, config.CompactOption{})
				require.NoErrorf(t, err, "could not compact at revision %d", currentRevision)

				afterCompactionTestCases := []rangeTestCase{
					{
						description: "the keys are deleted from the current revision even after compaction",
						key:         "",
						options: config.GetOptions{
							Prefix: true,
						},
						wantResponse: &clientv3.GetResponse{
							Count: 9,
							Kvs: []*mvccpb.KeyValue{
								kv("aa", "bar").createRev(firstRev + 13).modRev(firstRev + 13).version(1).build(),
								kv("b", "foob").createRev(firstRev + 2).modRev(firstRev + 2).version(1).build(),
								kv("c/abc", "egg").createRev(firstRev + 14).modRev(firstRev + 14).version(1).build(),
								kv("c/xyz", "4ever").createRev(firstRev + 9).modRev(firstRev + 9).version(1).build(),
								kv("d", "food").createRev(firstRev + 6).modRev(firstRev + 6).version(1).build(),
								kv("e", "fooe").createRev(firstRev + 7).modRev(firstRev + 7).version(1).build(),
								kv("f", "foof").createRev(firstRev + 8).modRev(firstRev + 8).version(1).build(),
								kv("x", "foox").createRev(firstRev + 1).modRev(firstRev + 1).version(1).build(),
								kv("y", "fooy").createRev(firstRev + 3).modRev(firstRev + 3).version(1).build(),
							},
						},
					},
					{
						description: "The revision has already been compacted",
						key:         "",
						options: config.GetOptions{
							Prefix:   true,
							Revision: firstRev + 14,
						},
						wantError: rpctypes.ErrCompacted,
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
					{
						description: "after compaction, original version is no longer available, with error",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 4,
						},
						wantError: rpctypes.ErrCompacted,
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
					{
						description: "after compaction, first update is no longer available either, with error",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12,
						},
						wantError: rpctypes.ErrCompacted,
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
					{
						description: "after compaction, second update is no longer available either, with error",
						key:         "c",
						options: config.GetOptions{
							Revision: firstRev + 12,
						},
						wantError: rpctypes.ErrCompacted,
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
					{
						description: "current version no longer has the deleted multiversioned entry",
						key:         "c",
						options:     config.GetOptions{},
						wantResponse: &clientv3.GetResponse{
							Count: 0,
							Kvs:   nil,
						},
					},
				}
				verify(t, afterCompactionTestCases)
			})
		})
	}
}

func TestKVDelete(t *testing.T) {
	testRunner.BeforeTest(t)
	for _, tc := range clusterTestCases() {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			clus := testRunner.NewCluster(ctx, t, config.WithClusterConfig(tc.config))
			defer clus.Close()
			cc := testutils.MustClient(clus.Client())
			testutils.ExecuteUntil(ctx, t, func() {
				kvs := []string{"a", "b", "c", "c/abc", "d"}
				tests := []struct {
					deleteKey string
					options   config.DeleteOptions

					wantDeleted int
					wantKeys    []string
				}{
					{ // delete all keys
						deleteKey:   "",
						options:     config.DeleteOptions{Prefix: true},
						wantDeleted: 5,
					},
					{ // delete all keys
						deleteKey:   "",
						options:     config.DeleteOptions{FromKey: true},
						wantDeleted: 5,
					},
					{
						deleteKey:   "a",
						options:     config.DeleteOptions{End: "c"},
						wantDeleted: 2,
						wantKeys:    []string{"c", "c/abc", "d"},
					},
					{
						deleteKey:   "c",
						wantDeleted: 1,
						wantKeys:    []string{"a", "b", "c/abc", "d"},
					},
					{
						deleteKey:   "c",
						options:     config.DeleteOptions{Prefix: true},
						wantDeleted: 2,
						wantKeys:    []string{"a", "b", "d"},
					},
					{
						deleteKey:   "c",
						options:     config.DeleteOptions{FromKey: true},
						wantDeleted: 3,
						wantKeys:    []string{"a", "b"},
					},
					{
						deleteKey:   "e",
						wantDeleted: 0,
						wantKeys:    kvs,
					},
				}
				for _, tt := range tests {
					for i := range kvs {
						_, err := cc.Put(ctx, kvs[i], "bar", config.PutOptions{})
						require.NoErrorf(t, err, "count not put key %q", kvs[i])
					}
					del, err := cc.Delete(ctx, tt.deleteKey, tt.options)
					require.NoErrorf(t, err, "count not get key %q, err", tt.deleteKey)
					assert.Equal(t, tt.wantDeleted, int(del.Deleted))
					get, err := cc.Get(ctx, "", config.GetOptions{Prefix: true})
					require.NoErrorf(t, err, "count not get key")
					kvs := testutils.KeysFromGetResponse(get)
					assert.Equal(t, tt.wantKeys, kvs)
				}
			})
		})
	}
}

func TestKVGetNoQuorum(t *testing.T) {
	testRunner.BeforeTest(t)
	tcs := []struct {
		name    string
		options config.GetOptions

		wantError bool
	}{
		{
			name:    "Serializable",
			options: config.GetOptions{Serializable: true},
		},
		{
			name:      "Linearizable",
			options:   config.GetOptions{Serializable: false, Timeout: time.Second},
			wantError: true,
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			clus := testRunner.NewCluster(ctx, t)
			defer clus.Close()

			clus.Members()[0].Stop()
			clus.Members()[1].Stop()

			cc := clus.Members()[2].Client()
			testutils.ExecuteUntil(ctx, t, func() {
				key := "foo"
				_, err := cc.Get(ctx, key, tc.options)
				if tc.wantError {
					require.Error(t, err)
				} else {
					require.NoError(t, err)
				}
			})
		})
	}
}
