package rpc

import (
	"fmt"
	"reflect"
	"strings"
	"sync"
)

// Service represents a registered RPC service.
type Service struct {
	Name    string
	Receiver interface{}
	Type    reflect.Type
	Methods map[string]*MethodType
}

// MethodType holds metadata about a service method.
type MethodType struct {
	Method    reflect.Method
	ArgType   reflect.Type
	ReplyType reflect.Type
}

// Registry manages service registration and discovery.
type Registry struct {
	mu       sync.RWMutex
	services map[string]*Service
}

func NewRegistry() *Registry {
	return &Registry{
		services: make(map[string]*Service),
	}
}

// Register inspects a receiver value and registers all exported methods.
func (r *Registry) Register(name string, receiver interface{}) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	t := reflect.TypeOf(receiver)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("receiver must be a pointer")
	}

	s := &Service{
		Name:     name,
		Receiver: receiver,
		Type:     t,
		Methods:  make(map[string]*MethodType),
	}

	for i := 0; i < t.NumMethod(); i++ {
		method := t.Method(i)
		if !method.IsExported() {
			continue
		}

		mt := &MethodType{
			Method:  method,
			ArgType: method.Type.In(1).Elem(),  // first arg (after receiver)
			ReplyType: method.Type.In(2).Elem(), // second arg
		}
		s.Methods[method.Name] = mt
	}

	r.services[name] = s
	return nil
}

// Get retrieves a service by name.
func (r *Registry) Get(name string) (*Service, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.services[name]
	return s, ok
}

// Call invokes a method on a registered service by reflection.
func (r *Registry) Call(serviceMethod string, args interface{}) (interface{}, error) {
	dot := strings.LastIndex(serviceMethod, ".")
	if dot < 0 {
		return nil, fmt.Errorf("invalid service method: %s (expected Service.Method)", serviceMethod)
	}

	serviceName := serviceMethod[:dot]
	methodName := serviceMethod[dot+1:]

	svc, ok := r.Get(serviceName)
	if !ok {
		return nil, fmt.Errorf("service not found: %s", serviceName)
	}

	mt, ok := svc.Methods[methodName]
	if !ok {
		return nil, fmt.Errorf("method not found: %s.%s", serviceName, methodName)
	}

	argVal := reflect.ValueOf(args).Elem()
	replyVal := reflect.New(mt.ReplyType)
	returnVals := mt.Method.Func.Call([]reflect.Value{
		reflect.ValueOf(svc.Receiver),
		argVal,
		replyVal,
	})

	if len(returnVals) > 0 {
		if err, ok := returnVals[0].Interface().(error); ok && err != nil {
			return nil, err
		}
	}

	return replyVal.Interface(), nil
}

// List returns all registered service names.
func (r *Registry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, 0, len(r.services))
	for name := range r.services {
		names = append(names, name)
	}
	return names
}
