package appsec

import org.apache.flink.api.common.io.FileInputFormat
import org.apache.flink.api.common.serialization.SerializerConfigImpl
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.core.fs.FileInputSplit
import org.apache.flink.core.fs.Path
import org.apache.flink.core.memory.DataInputViewStreamWrapper
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import java.io.File


class SerializedObjectInputFormat<T>(filePath: Path?, private val typeInfo: TypeInformation<T>) :
    FileInputFormat<T>(filePath) {
    @Transient
    private var serializer: TypeSerializer<T>? = null

    override fun open(split: FileInputSplit) {
        super.open(split)
        val serializerConfig = SerializerConfigImpl()
        serializer = typeInfo.createSerializer(serializerConfig)
    }

    override fun reachedEnd(): Boolean {
        return stream.available() <= 0
    }

    override fun nextRecord(reuse: T): T {
        return serializer!!.deserialize(DataInputViewStreamWrapper(stream))
    }
}


fun main() {
    val env = StreamExecutionEnvironment.getExecutionEnvironment()
    val wrapperTypeInfo = Types.POJO(DataInput::class.java)
    val path = "file://${File("exploit.kryo").absolutePath}"
    val inputFormat = SerializedObjectInputFormat(Path(path), wrapperTypeInfo)
    val stream = env.createInput(inputFormat, Types.POJO(DataInput::class.java))
        .map { it as DataInput }
    stream.print()
    env.execute("flink rce kyro")

}
