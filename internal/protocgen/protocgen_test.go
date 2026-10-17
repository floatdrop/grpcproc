package protocgen_test

import (
	"os"
	"strings"
	"testing"

	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/pluginpb"

	"github.com/floatdrop/grpcproc/internal/protocgen"
	grpcprocv1 "github.com/floatdrop/grpcproc/proto/grpcproc/v1"
)

// request is what protoc would hand the plugin for the test protos: their
// descriptors as buf build wrote them to testdata, comments included, after
// what they import, with gen.proto changed by edit, if any.
func request(t *testing.T, edit func(*descriptorpb.FileDescriptorProto)) *pluginpb.CodeGeneratorRequest {
	t.Helper()
	text, err := os.ReadFile("testdata/image.txtpb")
	if err != nil {
		t.Fatal(err)
	}
	var set descriptorpb.FileDescriptorSet
	if err := prototext.Unmarshal(text, &set); err != nil {
		t.Fatal(err)
	}
	req := &pluginpb.CodeGeneratorRequest{
		FileToGenerate: []string{"protocgen/testpb/gen.proto", "protocgen/testpb/nosvc.proto"},
		Parameter:      proto.String("module=github.com/floatdrop/grpcproc"),
		ProtoFile: append([]*descriptorpb.FileDescriptorProto{
			protodesc.ToFileDescriptorProto(descriptorpb.File_google_protobuf_descriptor_proto),
			protodesc.ToFileDescriptorProto(emptypb.File_google_protobuf_empty_proto),
			protodesc.ToFileDescriptorProto(grpcprocv1.File_grpcproc_v1_options_proto),
		}, set.File...),
	}
	for _, f := range set.File {
		if f.GetName() == "protocgen/testpb/gen.proto" && edit != nil {
			edit(f)
		}
	}
	return req
}

func run(t *testing.T, req *pluginpb.CodeGeneratorRequest) *pluginpb.CodeGeneratorResponse {
	t.Helper()
	gen, err := protogen.Options{}.New(req)
	if err != nil {
		t.Fatal(err)
	}
	if err := protocgen.Generate(gen); err != nil {
		gen.Error(err)
	}
	return gen.Response()
}

// The plugin writes what is checked in beside the test protos, which buf
// generate keeps current: one file, for the file with processes in it.
func TestGenerate(t *testing.T) {
	resp := run(t, request(t, nil))
	if resp.Error != nil {
		t.Fatal(resp.GetError())
	}
	if f := pluginpb.CodeGeneratorResponse_Feature(resp.GetSupportedFeatures()); f&pluginpb.CodeGeneratorResponse_FEATURE_PROTO3_OPTIONAL == 0 || f&pluginpb.CodeGeneratorResponse_FEATURE_SUPPORTS_EDITIONS == 0 {
		t.Errorf("supported features %v", f)
	}
	if len(resp.File) != 1 || resp.File[0].GetName() != "internal/protocgen/testpb/gen_grpcproc.pb.go" {
		t.Fatalf("files: %v", resp.File)
	}
	want, err := os.ReadFile("testpb/gen_grpcproc.pb.go")
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.File[0].GetContent(); got != string(want) {
		t.Fatalf("generated code differs from testpb/gen_grpcproc.pb.go: run buf generate\n%s", got)
	}
	code := resp.File[0].GetContent()
	for _, want := range []string{"const EchoProcess = \"echo\"", "func EchoAt(node string) EchoAddr", "func RoomAddrOf(", "func (c EchoAddr) Note(", "case *Note:"} {
		if !strings.Contains(code, want) {
			t.Errorf("missing %q", want)
		}
	}
	if strings.Contains(code, "Plain") || strings.Contains(code, "RoomAt(") {
		t.Error("a gRPC service, or an At for a process with no name, was generated")
	}
}

// A streaming rpc, and a send that returns something, are refused with the
// rpc's name.
func TestGenerateRefuses(t *testing.T) {
	method := func(f *descriptorpb.FileDescriptorProto, name string) *descriptorpb.MethodDescriptorProto {
		for _, s := range f.Service {
			for _, m := range s.Method {
				if m.GetName() == name {
					return m
				}
			}
		}
		t.Fatalf("no rpc %s", name)
		return nil
	}
	for _, tc := range []struct {
		edit func(*descriptorpb.FileDescriptorProto)
		want string
	}{
		{func(f *descriptorpb.FileDescriptorProto) { method(f, "Bounce").ServerStreaming = proto.Bool(true) }, "Room.Bounce: a process takes messages and answers calls: no streaming"},
		{func(f *descriptorpb.FileDescriptorProto) {
			method(f, "Note").OutputType = proto.String(".grpcproc.protocgen.test.v1.Pong")
		}, "Echo.Note: a send answers nothing: it returns google.protobuf.Empty, not grpcproc.protocgen.test.v1.Pong"},
		{func(f *descriptorpb.FileDescriptorProto) { method(f, "Bounce").Name = proto.String("Send") }, "Room.Send: an rpc may not be named as a method of grpcproc.Addr"},
		{func(f *descriptorpb.FileDescriptorProto) {
			s := f.Service[0]
			s.Method = append(s.Method, &descriptorpb.MethodDescriptorProto{Name: proto.String("Again"), InputType: proto.String(".grpcproc.protocgen.test.v1.Ping"), OutputType: proto.String(".grpcproc.protocgen.test.v1.Pong")})
		}, "Echo.Again: Ping takes grpcproc.protocgen.test.v1.Ping too: a message is dispatched by its type, to one rpc"},
	} {
		resp := run(t, request(t, tc.edit))
		if !strings.Contains(resp.GetError(), tc.want) {
			t.Errorf("error %q, want %q", resp.GetError(), tc.want)
		}
	}
}
