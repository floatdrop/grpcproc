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
	for _, want := range []string{
		"const EchoProcess = \"echo\"", "func EchoAt(node string) EchoAddr", "func EchoGlobal() EchoAddr",
		"func (c EchoAddr) Note(", "case *Note:", "func (c EchoAddr) NoteAfter[M proto.Message](p *grpcproc.Process[M], d time.Duration, req *Note) *grpcproc.Timer",
		"func (c EchoAddr) Count(ctx context.Context, from grpcproc.Caller) (*Pong, error)", "func (c EchoAddr) Clear(ctx context.Context, from grpcproc.Caller) error",
		"func (c EchoAddr) ClearAfter[M proto.Message](p *grpcproc.Process[M], d time.Duration) *grpcproc.Timer",
		"Count(p *grpcproc.Process[proto.Message], m grpcproc.Msg[proto.Message]) (*Pong, error)",
		"const EchoStatsTopic = \"echo-stats\"", "func EchoStatsAt(node string) pubsub.Topic[*Pong]",
		"type UnimplementedEchoHandler struct{}", "func (UnimplementedEchoHandler) Ping(",
		"func RoomProcess(id string) string { return \"room:\" + id }", "func RoomAt(node, id string) RoomAddr", "func RoomGlobal(id string) RoomAddr",
		"func RoomEventsTopic(id string) string { return RoomProcess(id) + \"/events\" }", "func RoomEventsAt(node, id string) pubsub.Topic[*Note]",
		"func RoomAddrOf(", "func SinkAddrOf(",
		"func HandleSinkCall(s SinkHandler, p *grpcproc.Process[proto.Message], m grpcproc.Msg[proto.Message]) (proto.Message, error) {\n\treturn nil, grpcproc.ErrType",
		"switch m.Body.(type) {\n\tcase *Clear:\n\t\treturn s.Flush(p, m)", "func (c SinkAddr) FlushAfter[M proto.Message](p *grpcproc.Process[M], d time.Duration) *grpcproc.Timer",
		"func (UnimplementedEchoHandler) Ping(*grpcproc.Process[proto.Message], grpcproc.Msg[proto.Message], *Ping) (*Pong, error)",
		"// EchoStatsAt is the Stats topic of the Echo service on node, whose events are Pong.\n//\n// Stats is the topic",
	} {
		if !strings.Contains(code, want) {
			t.Errorf("missing %q", want)
		}
	}
	for _, wrong := range []string{"Plain", "RoomProcessAddr", "Stats(ctx", "Events(ctx", "SinkAt(", "SinkProcess", "SinkGlobal", "_ = req", "_, _ = p, m"} {
		if strings.Contains(code, wrong) {
			t.Errorf("generated %q: a gRPC service, the proto name of a renamed one, or a topic as a method", wrong)
		}
	}
}

// What the rules leave alone: a topic named as a method of Addr, since it
// becomes no method; a call beside a send named after it with After, since
// only a send has a timer; and a request with an extension range, which
// carries something though it has no fields.
func TestGenerateAllows(t *testing.T) {
	resp := run(t, request(t, func(f *descriptorpb.FileDescriptorProto) {
		method(t, f, "Stats").Name = proto.String("Name")
		after := &descriptorpb.MethodDescriptorProto{Name: proto.String("CountAfter"), InputType: proto.String(".grpcproc.protocgen.test.v1.Ping"), OutputType: proto.String(".google.protobuf.Empty"), Options: &descriptorpb.MethodOptions{}}
		proto.SetExtension(after.Options, grpcprocv1.E_Send, true)
		f.Service[0].Method = append(f.Service[0].Method, after)
		f.Syntax = proto.String("proto2") // where an extension range may be declared
		for _, m := range f.MessageType {
			if m.GetName() == "Clear" {
				m.ExtensionRange = append(m.ExtensionRange, &descriptorpb.DescriptorProto_ExtensionRange{Start: proto.Int32(100), End: proto.Int32(200)})
			}
		}
	}))
	if resp.Error != nil {
		t.Fatal(resp.GetError())
	}
	code := resp.File[0].GetContent()
	for _, want := range []string{"func EchoNameAt(node string)", "func (c EchoAddr) CountAfter(ctx", "func (c EchoAddr) Clear(ctx context.Context, from grpcproc.Caller, req *Clear) error"} {
		if !strings.Contains(code, want) {
			t.Errorf("missing %q", want)
		}
	}
}

// A streaming rpc, and a send that returns something, are refused with the
// rpc's name.
// method is the rpc named name, in whichever service of f has it.
func method(t *testing.T, f *descriptorpb.FileDescriptorProto, name string) *descriptorpb.MethodDescriptorProto {
	t.Helper()
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

func TestGenerateRefuses(t *testing.T) {
	for _, tc := range []struct {
		edit func(*descriptorpb.FileDescriptorProto)
		want string
	}{
		{func(f *descriptorpb.FileDescriptorProto) { method(t, f, "Bounce").ServerStreaming = proto.Bool(true) }, "RoomProcess.Bounce: a process takes messages and answers calls: no streaming"},
		{func(f *descriptorpb.FileDescriptorProto) {
			method(t, f, "Note").OutputType = proto.String(".grpcproc.protocgen.test.v1.Pong")
		}, "Echo.Note: a send answers nothing: it returns google.protobuf.Empty, not grpcproc.protocgen.test.v1.Pong"},
		{func(f *descriptorpb.FileDescriptorProto) { method(t, f, "Bounce").Name = proto.String("Send") }, "RoomProcess.Send: an rpc may not be named as a method of grpcproc.Addr or of an actor"},
		{func(f *descriptorpb.FileDescriptorProto) { method(t, f, "Bounce").Name = proto.String("Init") }, "RoomProcess.Init: an rpc may not be named"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(f.Service[0].Options, grpcprocv1.E_Process, &grpcprocv1.ProcessOptions{Name: "echo", Prefix: "echo:"})
		}, "Echo: a process has a name or a prefix, not both"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(method(t, f, "Stats").Options, grpcprocv1.E_Topic, &grpcprocv1.TopicOptions{})
		}, "Echo.Stats: a topic needs a name"},
		{func(f *descriptorpb.FileDescriptorProto) {
			method(t, f, "Stats").InputType = proto.String(".grpcproc.protocgen.test.v1.Ping")
		}, "Echo.Stats: a topic takes google.protobuf.Empty and returns its events: it takes grpcproc.protocgen.test.v1.Ping"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(f.Service[1].Options, grpcprocv1.E_Process, &grpcprocv1.ProcessOptions{GoName: "Room"})
		}, "RoomProcess.Events: a topic is named after its process, which has no name and no prefix"},
		{func(f *descriptorpb.FileDescriptorProto) { method(t, f, "Bounce").Name = proto.String("Addr") }, "RoomProcess.Addr: an rpc may not be named"},
		{func(f *descriptorpb.FileDescriptorProto) {
			s := f.Service[0]
			s.Method = append(s.Method, &descriptorpb.MethodDescriptorProto{Name: proto.String("NoteAfter"), InputType: proto.String(".grpcproc.protocgen.test.v1.Ping"), OutputType: proto.String(".grpcproc.protocgen.test.v1.Pong")})
		}, "Echo.NoteAfter: it would generate the method NoteAfter, which Note does too"},
		{func(f *descriptorpb.FileDescriptorProto) {
			f.MessageType = append(f.MessageType, &descriptorpb.DescriptorProto{Name: proto.String("EchoStatsTopic")})
		}, "Echo: it would generate EchoStatsTopic, which message grpcproc.protocgen.test.v1.EchoStatsTopic is named already"},
		{func(f *descriptorpb.FileDescriptorProto) {
			load := &descriptorpb.MethodDescriptorProto{Name: proto.String("Load"), InputType: proto.String(".google.protobuf.Empty"), OutputType: proto.String(".grpcproc.protocgen.test.v1.Note"), Options: &descriptorpb.MethodOptions{}}
			proto.SetExtension(load.Options, grpcprocv1.E_Topic, &grpcprocv1.TopicOptions{Name: "echo-stats"})
			f.Service[0].Method = append(f.Service[0].Method, load)
		}, "Echo.Load: Stats is the topic \"echo-stats\" too"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(f.Service[1].Options, grpcprocv1.E_Process, &grpcprocv1.ProcessOptions{Prefix: "room:", GoName: "room"})
		}, "RoomProcess: go_name \"room\" is not an exported Go identifier"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(f.Service[1].Options, grpcprocv1.E_Process, &grpcprocv1.ProcessOptions{Prefix: "room:", GoName: "Room_"})
		}, "RoomProcess: it would generate Room_Addr, which oneof field grpcproc.protocgen.test.v1.Room.addr is named already"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(method(t, f, "Stats").Options, grpcprocv1.E_Send, true)
		}, "Echo.Stats: a topic is not a send: one option or the other"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(method(t, f, "Stats").Options, grpcprocv1.E_Topic, &grpcprocv1.TopicOptions{Name: "echo"})
		}, "Echo: the Stats topic of Echo registers as \"echo\", which the Echo process does too"},
		{func(f *descriptorpb.FileDescriptorProto) {
			proto.SetExtension(f.Service[2].Options, grpcprocv1.E_Process, &grpcprocv1.ProcessOptions{Name: "echo"})
		}, "Sink: the Sink process registers as \"echo\", which the Echo process does too"},
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
