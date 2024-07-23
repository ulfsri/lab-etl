import zipfile
import csv
import struct
from datetime import datetime

path = "tests/test_files/STA/Ford_3FMT_Carpet_STA_N2_10K_240603_R1.ngb-ss3"

# function used for removing nested
# lists in python using recursion
output = []


def reemovNestings(l):
    for i in l:
        if type(i) == list:
            reemovNestings(i)
        else:
            # if i != b'':
            # if i.hex().startswith(
            #     "fffeff"
            # ):  # this mean the following bytes are a string
            #     x = i.hex()[8:]
            #     try:
            #         output.append(
            #             "Text: " + bytes.fromhex(x).decode("ascii", errors="ignore")
            #         )
            #     except UnicodeDecodeError:
            #         print(f"Failed to decode: {x}")
            #         output.append('Raw: "' + i.hex() + '"')
            # # elif len(i.hex())==8:
            # #     output.append("Float32: \"" + str(struct.unpack('<f', i)[0]) + "\"")
            # # elif len(i.hex())==16:
            # #     output.append(struct.unpack('<d', i)[0])
            # else:
            #     output.append('Raw: "' + i.hex() + '"')
            output.append(i.hex())


with zipfile.ZipFile(path, "r") as z:
    for file in z.filelist:
        # if file.filename.endswith('.table'):
        if file.filename == "Streams/stream_1.table":
            with z.open(
                file.filename
            ) as stream:  # at this point we've opened our stream table
                stream_table = stream.read()
                if b"\x00\x18\xfc\xff\xff\x03" in stream_table:
                    split1 = stream_table.split(
                        b"\x00\x18\xfc\xff\xff\x03"
                    )  # \x00\x18\xfc\xff\xff\x03 seems to be some sort of delimiter
                    # print(split1)
                    # split1 = stream_table.split(b'\x17\xfc\xff\xff')
                    for idx, x in enumerate(split1):
                        split2 = x.split(b"\x80\x01")
                        for idx_2, x_2 in enumerate(split2):
                            # print(x_2)
                            split2[idx_2] = x_2.split(
                                b"\x00\x00\x01\x00\x00\x00\x0c\x00\x17\xfc\xff\xff"
                            )
                            for idx_3, x_3 in enumerate(split2[idx_2]):
                                split2[idx_2][idx_3] = x_3.split(
                                    b"\x01\x00\x00\x00\x02\x00\x01\x00\x00"
                                )
                                for idx_4, x_4 in enumerate(split2[idx_2][idx_3]):
                                    split2[idx_2][idx_3][idx_4] = x_4.split(b"\x00\x03")
                                    for idx_5, x_5 in enumerate(
                                        split2[idx_2][idx_3][idx_4]
                                    ):
                                        split2[idx_2][idx_3][idx_4][idx_5] = x_5.split(
                                            b"\x00\x01\x00\x00\x00\x17\xfc\xff\xff\x0c\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01\x00\x18\xfc\xff\xff"
                                        )
                                        for idx_6, x_6 in enumerate(
                                            split2[idx_2][idx_3][idx_4][idx_5]
                                        ):
                                            split2[idx_2][idx_3][idx_4][idx_5][
                                                idx_6
                                            ] = x_6.split(
                                                b"\x00\x01\x00\x00\x00\x17\xfc\xff\xff\x0c\x00\x01\x00\x01\x00\x00\x00\x01\x00\x00\x00\x01"
                                            )
                                            for idx_7, x_7 in enumerate(
                                                split2[idx_2][idx_3][idx_4][idx_5][
                                                    idx_6
                                                ]
                                            ):
                                                split2[idx_2][idx_3][idx_4][idx_5][
                                                    idx_6
                                                ][idx_7] = x_7.split(
                                                    b"\x01\x80\x02\x00\x00\x80"
                                                )

                                # for idx_3, x_3 in enumerate(split2[idx_2]):
                                #     if b'\x01\x00\x00\x00\x02\x00\x01\x00' in x_3:
                                #         split2[idx_2][idx_3] = x_3.split(b'\x01\x00\x00\x00\x02\x00\x01\x00\x00')
                            # if b'\x01\x00\x00\x00\x02\x00\x01\x00' in x_2:
                            #     split2[idx_2] = split2[idx_2].split(b'\x01\x00\x00\x00\x02\x00\x01\x00\x00')
                        split1[idx] = split2

                        # for idx_3, x_3 in enumerate(split2):
                        #     split2[idx_3] = x_3.split(b'\x01\x00\x00\x00\x02\x00\x01\x00\x00')
                        # split1[idx] = split2

                for i in split1:
                    print(i)

                for idx, x in enumerate(split1):
                    output = []
                    reemovNestings(x)
                    # print(output)
                    split1[idx] = output
                for idx, x in enumerate(split1):
                    if len(x) > 3:
                        if x[2] == "1f":  # string
                            split1[idx][3] = (
                                'Text: "'
                                + bytes.fromhex(x[3][8:]).decode(
                                    "ascii", errors="ignore"
                                )
                                + '"'
                            )
                        elif x[2] == "04":  # float
                            split1[idx][3] = (
                                'Float32: "'
                                + str(struct.unpack("<f", bytes.fromhex(x[3]))[0])
                                + '"'
                            )
                        elif x[2] == "05":  # double
                            split1[idx][3] = (
                                'Float64: "'
                                + str(struct.unpack("<d", bytes.fromhex(x[3]))[0])
                                + '"'
                            )
                        elif x[2] == "03":  # int32
                            if (
                                (x[1] == "3e08")
                                | (x[1] == "3518")
                                | (x[1] == "fe1c")
                                | (x[1] == "0718")
                            ):  # datetime
                                time = struct.unpack("<i", bytes.fromhex(x[3]))[0]
                                dt = datetime.utcfromtimestamp(time).strftime(
                                    "%Y-%m-%d %H:%M:%S"
                                )
                                split1[idx][3] = 'DateTime: "' + dt + '"'
                            else:
                                split1[idx][3] = (
                                    'Int32: "'
                                    + str(struct.unpack("<i", bytes.fromhex(x[3]))[0])
                                    + '"'
                                )
                        elif x[2] == "02":  # int16
                            split1[idx][3] = (
                                'Int16: "'
                                + str(struct.unpack("<h", bytes.fromhex(x[3]))[0])
                                + '"'
                            )
                        elif x[2] == "10":  # bool
                            split1[idx][3] = (
                                'Bool: "'
                                + str(struct.unpack("<?", bytes.fromhex(x[3]))[0])
                                + '"'
                            )
                        elif x[2] == "1a":  # idk?
                            # print(x)
                            try:
                                split1[idx][4] = (
                                    'Unknown: "'
                                    + str(struct.unpack("<i", bytes.fromhex(x[4]))[0])
                                    + '"'
                                )
                            except struct.error:
                                split1[idx][4] = 'Unknown: "' + x[4] + '"'

                # for i in split1:
                #     print(i)
                with open("output.csv", "w") as f:
                    # using csv.writer method from CSV package
                    write = csv.writer(f)

                    write.writerows(split1)
