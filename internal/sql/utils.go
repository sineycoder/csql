package sql

func IfThen[T any](ok bool, ret1, ret2 T) T {
	if ok {
		return ret1
	}
	return ret2
}
