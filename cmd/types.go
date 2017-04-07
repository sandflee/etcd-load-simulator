package cmd

type Del struct {
	key string
}

type Get struct {
	key string
}

type List struct {
	prefix string
	start string
	end string
}

type Update struct {
	key string
	value string
	modIndex int
}

type Put struct  {
	key string
	value string
}

type Op struct {
	get *Get
	list *List
	put *Put
	update *Update
	del *Del
}
