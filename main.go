/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main

import (
	f "github.com/fauna/faunadb-go/faunadb"
	"log"
)

func main() {
	/*
	 * Create the admin client which we will use to create the first level DB
	 */
	secret := "secret"
	endpoint := f.Endpoint("http://127.0.0.1:8443")
	adminClient := f.NewFaunaClient(secret, endpoint)

	dbName := "LedgerExample"

	res, err := adminClient.Query(
		f.If(
			f.Exists(f.Database(dbName)),
			f.Arr{
				f.Delete(f.Database(dbName)),
				f.CreateDatabase(f.Obj{"name": dbName})},
			f.CreateDatabase(f.Obj{"name": dbName})))


	if err != nil {
		panic(err)
	}
	log.Printf("Created DB: %s: \n%s", dbName, res)

	/*
	 * Get a server level key for the DB that we just created.
	 * This will be used for all of the subsequent interaction.
	 * Effectively, this means that only this client and the admin
	 * will have access to these resources.
	 */
	res, err = adminClient.Query(
		f.CreateKey(f.Obj{
			"database": f.Database(dbName),
			"role":     "server"}))

	if err == nil {
		err = res.At(f.ObjKey("secret")).Get(&secret)
	} else {
		panic(err)
	}

	client := adminClient.NewSessionClient(secret)


	/*
	 * Create the two class objects that we will use in this model
	 */
	classes := []string{"customers", "transactions"}
	res, err = client.Query(
		f.Map(
			f.Arr{"customers", "transactions"},
			f.Lambda("c", f.CreateClass(f.Obj{"name": f.Var("c")}))))

	if err != nil {
		panic(err)
	}
	log.Printf("Created Classes %s: \n%s", classes, res)

	/*
	 * Create an index to access customer records by id
	 */
	res, err = client.Query(
		f.CreateIndex(f.Obj{
			"name": "customer_by_id",
			"source": f.Class("customers"),
			"unique": true,
			"terms": f.Obj{"field": f.Arr{"data", "id"}}}))

	if err != nil {
		panic(err)
	}
	log.Printf("Created Index'customer_by_id': %n%s", classes, res)

	/*
	 * Create a customer (record)
	 */
	custID := 0
	res, err = client.Query(
		f.Create(f.Class("customers"),  f.Obj{"data": f.Obj{"id": custID, "balance": 100}}))
	if err != nil {
		panic(err)
	}
	log.Printf("Create 'customer': %v \n%s", custID, res)

	/*
	 * Read a customer (record)
	 */
	res, err = client.Query(
		f.Select("data", f.Get(f.MatchTerm(f.Index("customer_by_id"), custID))))
	if err != nil {
		panic(err)
	}
	log.Printf("Read 'customer': %v \n%s", custID, res)

	/*
	 * Update a customer (record)
	 */
	res, err = client.Query(
		f.Update(
			f.Select("ref", f.Get(f.MatchTerm(f.Index("customer_by_id"), custID))),
			f.Obj{"data": f.Obj{"id": custID, "balance": 200}}))
	if err != nil {
		panic(err)
	}
	log.Printf("Update 'customer': %v \n%s", custID, res)

	/*
	 * Read a customer (record)
	 */
	res, err = client.Query(
		f.Select(f.Arr{"data", "balance"}, f.Get(f.MatchTerm(f.Index("customer_by_id"), custID))))
	if err != nil {
		panic(err)
	}
	log.Printf("Read 'customer': %v \n%s", custID, res)

	/*
	 * A more complex transaction example. A read before a write.
	 */
	withdrawal := 50
	res, err = client.Query(
		f.Let(
			f.Obj{
				"customer": f.Get(f.MatchTerm(f.Index("customer_by_id"), custID)),
			},
			f.Let(
				f.Obj{
					"origBalance": f.Select(f.Arr{"data", "balance"}, f.Var("customer")),
				},
				f.Let(
					f.Obj{
						"newBalance": f.Subtract(f.Var("origBalance"), withdrawal),
					},
					f.If(
						f.GTE(f.Var("newBalance"), 0),
						f.Update(
							f.Select("ref", f.Var("customer")),
							f.Obj{"data": f.Obj{"balance": f.Var("newBalance")}}),
						"Error. Insufficeint funds.")))))

	if err != nil {
		panic(err)
	}
	log.Printf("Read 'customer': %v \n%s", custID, res)

	/*
	 * Read a customer (record)
	 */
	res, err = client.Query(
		f.Select(f.Arr{"data", "balance"}, f.Get(f.MatchTerm(f.Index("customer_by_id"), custID))))
	if err != nil {
		panic(err)
	}
	log.Printf("Read 'customer': %v \n%s", custID, res)
}



