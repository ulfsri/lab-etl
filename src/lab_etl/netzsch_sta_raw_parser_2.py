import zipfile
import csv
import struct
from datetime import datetime

path = "tests/test_files/STA/Hyundai_KM8K_Carpet_STA_N2_10K_240711_R3.ngb-ss3"

# function used for removing nested
# lists in python using recursion
output = []


def reemovNestings(l):
    for i in l:
        if type(i) == list:
            reemovNestings(i)
        else:
            output.append(i.hex())


with zipfile.ZipFile(path, "r") as z:
    for file in z.filelist:
        # if file.filename.endswith('.table'):
        if file.filename == "Streams/stream_1.table":
            with z.open(file.filename) as stream:
                stream_table = stream.read()
                stream_table = stream_table.split(
                    b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
                )
                for idx, x in enumerate(stream_table):
                    stream_table[idx] = x.split(b"\x00\x18\xfc\xff\xff\x03")
                    for idx_2, x_2 in enumerate(stream_table[idx]):
                        if stream_table[idx][idx_2] == b"":
                            continue
                        stream_table[idx][idx_2] = x_2.split(b"\x80\x01")
                        for idx_3, x_3 in enumerate(stream_table[idx][idx_2]):
                            if stream_table[idx][idx_2][idx_3] == b"":
                                continue
                            stream_table[idx][idx_2][idx_3] = x_3.split(
                                b"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff"
                            )
                            for idx_4, x_4 in enumerate(
                                stream_table[idx][idx_2][idx_3]
                            ):
                                if stream_table[idx][idx_2][idx_3][idx_4] == b"":
                                    continue
                                stream_table[idx][idx_2][idx_3][idx_4] = x_4.split(
                                    b"\x00\x01\x00\x00\x00\x17\xfc\xff\xff\x0c\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01"
                                )
                                for idx_5, x_5 in enumerate(
                                    stream_table[idx][idx_2][idx_3][idx_4]
                                ):
                                    if (
                                        stream_table[idx][idx_2][idx_3][idx_4][idx_5]
                                        == b""
                                    ):
                                        continue
                                    stream_table[idx][idx_2][idx_3][idx_4][idx_5] = (
                                        x_5.split(b"\x00\x18\xfc\xff\xff")
                                    )
                for i in stream_table:
                    # print(i)
                    pass
                for idx, x in enumerate(stream_table):
                    output = []
                    reemovNestings(x)
                    # print(output)
                    stream_table[idx] = output
                for idx, x in enumerate(stream_table):
                    if x[0] == "0003":
                        if x[3] == "1f":  # string
                            stream_table[idx][4] = (
                                'Text: "'
                                + bytes.fromhex(x[4][8:]).decode(
                                    "ascii", errors="ignore"
                                )
                                + '"'
                            )
                        elif x[3] == "04":  # float
                            stream_table[idx][4] = (
                                'Float32: "'
                                + str(struct.unpack("<f", bytes.fromhex(x[4]))[0])
                                + '"'
                            )
                        elif x[3] == "05":  # double
                            stream_table[idx][4] = (
                                'Float64: "'
                                + str(struct.unpack("<d", bytes.fromhex(x[4]))[0])
                                + '"'
                            )
                        elif x[3] == "03":  # int32
                            if (
                                (x[2] == "3e08")
                                | (x[2] == "3518")
                                | (x[2] == "fe1c")
                                | (x[2] == "0718")
                            ):  # datetime
                                time = struct.unpack("<i", bytes.fromhex(x[4]))[0]
                                dt = datetime.utcfromtimestamp(time).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                stream_table[idx][4] = 'DateTime: "' + dt + '"'
                            else:
                                stream_table[idx][4] = (
                                    'Int32: "'
                                    + str(struct.unpack("<i", bytes.fromhex(x[4]))[0])
                                    + '"'
                                )
                        elif x[3] == "02":  # int16
                            stream_table[idx][4] = (
                                'Int16: "'
                                + str(struct.unpack("<h", bytes.fromhex(x[4]))[0])
                                + '"'
                            )
                        elif x[3] == "10":  # bool
                            stream_table[idx][4] = (
                                'Bool: "'
                                + str(struct.unpack("<?", bytes.fromhex(x[4]))[0])
                                + '"'
                            )
                        # elif x[3] == "1a":  # idk?
                        #     temp = (
                        #         stream_table[idx][4]
                        #         .replace("018002000080", "")
                        #         .replace("0000", "")
                        #     )
                        #     try:
                        #         stream_table[idx][4] = (
                        #             'Unknown: "'
                        #             + str(struct.unpack("<h", bytes.fromhex(temp))[0])
                        #             + '"'
                        #         )
                        #     except struct.error:
                        #         stream_table[idx][4] = 'Unknown: "' + temp + '"'
                        #     try:
                        #         cat = struct.unpack("<h", bytes.fromhex(x[2]))[0]
                        #         stream_table[idx][2] = 'Unknown: "' + str(cat) + '"'
                        #     except struct.error:
                        #         stream_table[idx][2] = 'Unknown: "' + x[2] + '"'
                        #     # print(x)
                        #     try:
                        #         stream_table[idx][4] = (
                        #             'Unknown: "'
                        #             + str(struct.unpack("<i", bytes.fromhex(x[4]))[0])
                        #             + '"'
                        #         )
                        #     except struct.error:
                        #         stream_table[idx][4] = 'Unknown: "' + x[4] + '"'
            # for idx, x in enumerate(stream_table):
            #     if len(x)>3:
            #         if x[3] == "1a":
            #             if '"5' in x[2]:
            #                 level = 0
            #             elif '"8' in x[2]:
            #                 level = 2
            #             elif '"6' in x[2]:
            #                 level = 3
            #             elif '"7' in x[2]:
            #                 level = 4
            #             elif '"3' in x[2]:
            #                 level = 5
            #             i = 1
            #             for j in range(level):
            #                 stream_table[idx].insert(1, "")
            #             try:
            #                 while stream_table[idx+i][3] != "1a":
            #                     for j in range(level):
            #                         stream_table[idx+i].insert(1, "")
            #                     i += 1
            #             except IndexError:
            #                 pass

            print(stream_table)
            with open("output5.csv", "w") as f:
                # using csv.writer method from CSV package
                write = csv.writer(f)

                write.writerows(stream_table)
